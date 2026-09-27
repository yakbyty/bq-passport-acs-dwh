-- M5: canonical ACS Parts Merchant Center current state
-- Project: supple-gearbox-470711-e3
-- Dataset: analytics_demand_acsparts
-- Merchant: 172724510 ONLY
-- Source: merchant_center_acsparts_raw
-- Safety: read-only transformations. No Merchant writes.

CREATE TEMP FUNCTION canonical_sku(value STRING)
RETURNS STRING
AS (
  CASE
    WHEN value IS NULL THEN NULL
    WHEN REGEXP_CONTAINS(UPPER(TRIM(value)), r'^P[0-9]+$')
      THEN CONCAT('Р', SUBSTR(UPPER(TRIM(value)), 2))
    ELSE UPPER(TRIM(value))
  END
);

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
CLUSTER BY canonical_sku
OPTIONS (
  description = 'Current ACS Parts Merchant product resources from latest Products_172724510 partition.'
)
AS
WITH latest_partition AS (
  SELECT MAX(DATE(_PARTITIONTIME)) AS partition_date
  FROM `supple-gearbox-470711-e3.merchant_center_acsparts_raw.Products_172724510`
),
current_partition AS (
  SELECT
    p.*,
    DATE(_PARTITIONTIME) AS source_partition_date
  FROM `supple-gearbox-470711-e3.merchant_center_acsparts_raw.Products_172724510` p
  CROSS JOIN latest_partition l
  WHERE DATE(p._PARTITIONTIME) = l.partition_date
    AND p.merchant_id = 172724510
),
dedup AS (
  SELECT *
  FROM current_partition
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id
    ORDER BY product_data_timestamp DESC, source_partition_date DESC
  ) = 1
)
SELECT
  merchant_id,
  product_id,
  offer_id,
  canonical_sku(offer_id) AS canonical_sku,
  title,
  description,
  link,
  mobile_link,
  image_link,
  additional_image_links,
  content_language,
  target_country,
  feed_label,
  channel,
  availability,
  availability_date,
  brand,
  mpn,
  gtin,
  condition,
  price.value AS price_value,
  price.currency AS price_currency,
  sale_price.value AS sale_price_value,
  sale_price.currency AS sale_price_currency,
  custom_labels.label_0 AS custom_label_0,
  custom_labels.label_1 AS custom_label_1,
  custom_labels.label_2 AS custom_label_2,
  custom_labels.label_3 AS custom_label_3,
  custom_labels.label_4 AS custom_label_4,
  ARRAY_LENGTH(destinations) AS destination_count,
  ARRAY_LENGTH(issues) AS issue_count,
  destinations,
  issues,
  product_data_timestamp,
  source_partition_date,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM dedup;

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_merchant_destinations_current`
CLUSTER BY canonical_sku
OPTIONS (
  description = 'Exploded current Merchant destination states for ACS Parts.'
)
AS
SELECT
  p.merchant_id,
  p.product_id,
  p.offer_id,
  p.canonical_sku,
  p.feed_label,
  p.content_language,
  p.target_country,
  p.source_partition_date,
  d.name AS destination,
  d.status AS destination_status,
  d.approved_countries,
  d.pending_countries,
  d.disapproved_countries,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current` p,
UNNEST(p.destinations) d;

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_merchant_issues_current`
CLUSTER BY canonical_sku
OPTIONS (
  description = 'Exploded current Merchant product issues for ACS Parts.'
)
AS
SELECT
  p.merchant_id,
  p.product_id,
  p.offer_id,
  p.canonical_sku,
  p.feed_label,
  p.content_language,
  p.target_country,
  p.source_partition_date,
  i.code AS issue_code,
  i.servability,
  i.resolution,
  i.attribute_name,
  i.destination,
  i.short_description,
  i.detailed_description,
  i.documentation,
  i.applicable_countries,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current` p,
UNNEST(p.issues) i;

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_offer_current`
CLUSTER BY canonical_sku
OPTIONS (
  description = 'Merchant coverage summary by canonical ACS offer/SKU across locales and feed labels.'
)
AS
SELECT
  canonical_sku,
  COUNT(*) AS product_resource_count,
  ARRAY_AGG(DISTINCT offer_id IGNORE NULLS ORDER BY offer_id) AS offer_ids,
  ARRAY_AGG(DISTINCT feed_label IGNORE NULLS ORDER BY feed_label) AS feed_labels,
  ARRAY_AGG(DISTINCT content_language IGNORE NULLS ORDER BY content_language) AS content_languages,
  ARRAY_AGG(DISTINCT target_country IGNORE NULLS ORDER BY target_country) AS target_countries,
  ARRAY_AGG(DISTINCT price_currency IGNORE NULLS ORDER BY price_currency) AS price_currencies,
  ARRAY_AGG(DISTINCT availability IGNORE NULLS ORDER BY availability) AS availability_states,
  COUNTIF(issue_count > 0) AS resources_with_issues,
  SUM(issue_count) AS issue_count,
  MAX(product_data_timestamp) AS last_product_update,
  MAX(source_partition_date) AS source_partition_date,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
WHERE canonical_sku IS NOT NULL
  AND canonical_sku != ''
GROUP BY canonical_sku;

CREATE OR REPLACE VIEW `supple-gearbox-470711-e3.analytics_demand_acsparts.v_merchant_source_health`
OPTIONS (
  description = 'ACS Parts Merchant raw/current health. Merchant 172724510 only.'
)
AS
WITH products AS (
  SELECT
    COUNT(*) AS current_product_resources,
    COUNT(DISTINCT canonical_sku) AS current_unique_sku,
    COUNTIF(issue_count > 0) AS product_resources_with_issues,
    SUM(issue_count) AS issue_count,
    MAX(product_data_timestamp) AS last_product_update,
    MAX(source_partition_date) AS products_partition_date,
    COUNTIF(merchant_id != 172724510) AS forbidden_merchant_rows
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_merchant_products_current`
),
targeting AS (
  SELECT
    MAX(DATE(_PARTITIONTIME)) AS targeting_partition_date,
    COUNTIF(DATE(_PARTITIONTIME) = (
      SELECT MAX(DATE(_PARTITIONTIME))
      FROM `supple-gearbox-470711-e3.merchant_center_acsparts_raw.ProductTargeting_172724510`
    )) AS targeting_rows_latest_partition
  FROM `supple-gearbox-470711-e3.merchant_center_acsparts_raw.ProductTargeting_172724510`
),
performance AS (
  SELECT
    MAX(DATE(_PARTITIONTIME)) AS performance_partition_date,
    COUNT(*) AS performance_rows_total
  FROM `supple-gearbox-470711-e3.merchant_center_acsparts_raw.ProductPerformance_172724510`
)
SELECT
  'MERCHANT_CENTER' AS source_id,
  172724510 AS expected_merchant_id,
  p.current_product_resources,
  p.current_unique_sku,
  p.product_resources_with_issues,
  p.issue_count,
  p.last_product_update,
  p.products_partition_date,
  t.targeting_partition_date,
  t.targeting_rows_latest_partition,
  perf.performance_partition_date,
  perf.performance_rows_total,
  p.forbidden_merchant_rows,
  CASE
    WHEN p.forbidden_merchant_rows > 0 THEN 'FAILED_SCOPE'
    WHEN p.products_partition_date IS NULL THEN 'FAILED_NO_PRODUCTS'
    WHEN DATE_DIFF(CURRENT_DATE('Europe/Kyiv'), p.products_partition_date, DAY) <= 1 THEN 'FRESH_QA_REQUIRED'
    WHEN DATE_DIFF(CURRENT_DATE('Europe/Kyiv'), p.products_partition_date, DAY) <= 2 THEN 'LAGGING'
    ELSE 'STALE'
  END AS health_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM products p
CROSS JOIN targeting t
CROSS JOIN performance perf;
