-- M6 PRE-FLIGHT: SalesDrive -> ACS Parts canonical order lifecycle
-- Project: supple-gearbox-470711-e3
-- READ ONLY: this file creates nothing and changes nothing.
-- Purpose: prove freshness, scope and attribution-field availability before any ACS mart is built.

-- 1. Required objects must exist.
WITH required AS (
  SELECT 'salesdrive_raw' AS dataset_name, 'orders_raw' AS table_name UNION ALL
  SELECT 'salesdrive_raw', 'payments_raw' UNION ALL
  SELECT 'salesdrive_raw', 'load_state' UNION ALL
  SELECT 'salesdrive_core', 'v_orders' UNION ALL
  SELECT 'salesdrive_core', 'v_order_items' UNION ALL
  SELECT 'salesdrive_core', 'dim_order_statuses' UNION ALL
  SELECT 'salesdrive_dwh', 'fact_orders' UNION ALL
  SELECT 'salesdrive_dwh', 'fact_payments'
),
inventory AS (
  SELECT table_schema AS dataset_name, table_name
  FROM `supple-gearbox-470711-e3.region-eu.INFORMATION_SCHEMA.TABLES`
)
SELECT
  r.*,
  IF(i.table_name IS NULL, 'FAIL_MISSING', 'PASS') AS object_status
FROM required r
LEFT JOIN inventory i USING(dataset_name, table_name)
ORDER BY dataset_name, table_name;

-- 2. Loader state / freshness.
SELECT
  entity,
  last_updated_at,
  updated_at AS loader_state_updated_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS source_lag_hours,
  CASE
    WHEN last_updated_at IS NULL THEN 'FAIL_NO_DATE'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 24 THEN 'FRESH'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 48 THEN 'LAGGING'
    ELSE 'STALE'
  END AS freshness_status
FROM `supple-gearbox-470711-e3.salesdrive_raw.load_state`
ORDER BY entity;

-- 3. Raw-table actual load timestamps.
SELECT 'orders_raw' AS source_object, MAX(loaded_at) AS max_loaded_at, COUNT(*) AS rows
FROM `supple-gearbox-470711-e3.salesdrive_raw.orders_raw`
UNION ALL
SELECT 'payments_raw', MAX(loaded_at), COUNT(*)
FROM `supple-gearbox-470711-e3.salesdrive_raw.payments_raw`;

-- 4. Order scope candidates. Nothing is allowed into ACS decision marts until
--    the acsparts.biz population is positively identified.
SELECT
  form_id,
  organization_id,
  status_id,
  COUNT(*) AS orders,
  MIN(SAFE_CAST(order_time AS TIMESTAMP)) AS first_order_at,
  MAX(SAFE_CAST(order_time AS TIMESTAMP)) AS last_order_at
FROM `supple-gearbox-470711-e3.salesdrive_core.v_orders`
GROUP BY form_id, organization_id, status_id
ORDER BY orders DESC;

-- 5. Probe raw payload for attribution/site fields.
--    These paths are candidates only. Their presence must be proven from real current payload.
WITH src AS (
  SELECT SAFE.PARSE_JSON(payload) AS j
  FROM `supple-gearbox-470711-e3.salesdrive_raw.orders_raw`
  WHERE payload IS NOT NULL
)
SELECT
  COUNT(*) AS raw_orders,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.site'), '') IS NOT NULL) AS with_site,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.utm_source'), '') IS NOT NULL) AS with_utm_source,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.utm_medium'), '') IS NOT NULL) AS with_utm_medium,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.utm_campaign'), '') IS NOT NULL) AS with_utm_campaign,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.utm_content'), '') IS NOT NULL) AS with_utm_content,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.utm_term'), '') IS NOT NULL) AS with_utm_term,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.gclid'), '') IS NOT NULL) AS with_gclid,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.gbraid'), '') IS NOT NULL) AS with_gbraid,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.wbraid'), '') IS NOT NULL) AS with_wbraid,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.source'), '') IS NOT NULL) AS with_source,
  COUNTIF(NULLIF(JSON_VALUE(j, '$.advertising_campaign'), '') IS NOT NULL) AS with_advertising_campaign
FROM src;

-- 6. Current order/item quality.
SELECT
  COUNT(*) AS orders,
  COUNT(DISTINCT order_id) AS distinct_orders,
  COUNTIF(order_id IS NULL) AS missing_order_id,
  COUNTIF(status_id IS NULL) AS missing_status,
  MAX(SAFE_CAST(order_time AS TIMESTAMP)) AS latest_order_at
FROM `supple-gearbox-470711-e3.salesdrive_core.v_orders`;

SELECT
  COUNT(*) AS item_rows,
  COUNT(DISTINCT order_id) AS orders_with_items,
  COUNTIF(order_id IS NULL) AS missing_order_id,
  COUNTIF(NULLIF(TRIM(sku), '') IS NULL) AS missing_sku,
  COUNTIF(quantity IS NULL OR quantity <= 0) AS invalid_quantity
FROM `supple-gearbox-470711-e3.salesdrive_core.v_order_items`;

-- ACCEPTANCE GATE:
-- A. all required objects PASS
-- B. orders source FRESH/LAGGING only; STALE = STOP
-- C. acsparts.biz scope is positively identified (form_id/org/site rule)
-- D. SKU/order keys have acceptable completeness
-- E. attribution fields are mapped from actual payload, not guessed
-- Until A-E pass, SalesDrive remains NO for ACS automated decisions.
