-- M5 verification for Merchant 172724510.
-- Read-only. Run after 20260927_m5_merchant_canonical.sql.

-- 1) Hard merchant isolation: must be 0.
SELECT COUNT(*) AS forbidden_merchant_rows
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
WHERE merchant_id != 172724510;

-- 2) One current row per product resource: duplicate count must be 0.
SELECT COUNT(*) AS duplicated_product_ids
FROM (
  SELECT product_id
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
  GROUP BY product_id
  HAVING COUNT(*) > 1
);

-- 3) Current source health.
SELECT *
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.v_merchant_source_health`;

-- 4) Audited live samples.
-- R290 was confirmed by live Merchant API on 2026-09-27.
-- R01/R04/R552/R604/R874 were not returned by exact server-side product_id filters.
SELECT
  canonical_sku,
  COUNT(*) AS product_resources,
  ARRAY_AGG(DISTINCT feed_label IGNORE NULLS ORDER BY feed_label) AS feed_labels,
  ARRAY_AGG(DISTINCT price_currency IGNORE NULLS ORDER BY price_currency) AS currencies,
  SUM(issue_count) AS issue_count,
  MAX(product_data_timestamp) AS last_update
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
WHERE canonical_sku IN ('Р290','Р01','Р04','Р552','Р604','Р874')
GROUP BY canonical_sku
ORDER BY canonical_sku;

-- 5) Largest issue classes for current products.
SELECT
  issue_code,
  destination,
  servability,
  COUNT(*) AS affected_product_resources
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_merchant_issues_current`
GROUP BY issue_code, destination, servability
ORDER BY affected_product_resources DESC, issue_code
LIMIT 100;

-- 6) Destination coverage.
SELECT
  destination,
  destination_status,
  COUNT(*) AS product_resources
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_merchant_destinations_current`
GROUP BY destination, destination_status
ORDER BY destination, product_resources DESC;
