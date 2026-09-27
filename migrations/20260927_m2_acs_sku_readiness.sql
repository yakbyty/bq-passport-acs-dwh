-- M2: canonical ACS Parts product master and SKU readiness
-- Project: supple-gearbox-470711-e3
-- Dataset: analytics_demand_acsparts
-- Safety: this migration does NOT modify Ads, Merchant, Search campaigns, or existing GA4 marts.
-- It adds three read-only Google Sheets external sources and two canonical tables.
-- Run as a BigQuery multi-statement script in EU.

CREATE OR REPLACE EXTERNAL TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_horoshop_products`
(
  sku_a STRING,
  sku_b STRING,
  sku_c STRING,
  title_ua_1 STRING,
  title_en_1 STRING,
  title_pl_1 STRING,
  title_de_1 STRING,
  title_es_1 STRING,
  title_ua_2 STRING,
  title_en_2 STRING,
  title_pl_2 STRING,
  title_de_2 STRING,
  title_es_2 STRING,
  brand STRING,
  category_path STRING,
  site_price_raw STRING,
  wholesale_price_1_raw STRING,
  wholesale_min_qty_1_raw STRING,
  old_price_raw STRING,
  currency STRING,
  displayed_raw STRING,
  site_availability_raw STRING,
  reserved_w STRING,
  image_main STRING,
  images_additional STRING,
  reserved_z STRING,
  product_alias STRING,
  product_url STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1rIYX8dzqSK23JbT2-8sINta3RZCvReCbjYYaCpIIeVg/edit?usp=sharing'],
  sheet_range = 'выгрузка!A:AB',
  skip_leading_rows = 4
);

CREATE OR REPLACE EXTERNAL TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_cogs_current`
(
  sku_raw STRING,
  title STRING,
  partner_price_raw STRING,
  base_cogs_raw STRING,
  base_source STRING,
  final_cogs_raw STRING,
  final_source STRING,
  control STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1DdoKlX0usA-WlvoJtz8JRaaJlEpmlldXShvWiqs6mjg/edit?usp=sharing'],
  sheet_range = 'COGS итог!A:H',
  skip_leading_rows = 1
);

CREATE OR REPLACE EXTERNAL TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_stock_current`
(
  sku_raw STRING,
  title STRING,
  stock_qty_raw STRING
)
OPTIONS (
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1fx9-LWIJyn-Bz44DP_sggBTJQX57q9DYXlsYTCuy9Yc/edit?usp=sharing'],
  sheet_range = 'Остатки ACS YML!A:C',
  skip_leading_rows = 1
);

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

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_master`
CLUSTER BY sku
OPTIONS (
  description = 'ACS Parts canonical current product state. Site price/URL/image from Horoshop, COGS from БД Цены/COGS итог, stock from Остатки ACS YML.'
)
AS
WITH
site_rows AS (
  SELECT
    canonical_sku(sku_a) AS sku,
    NULLIF(TRIM(title_ua_2), '') AS title_ua,
    COALESCE(NULLIF(TRIM(title_en_2), ''), NULLIF(TRIM(title_en_1), '')) AS title_en,
    NULLIF(TRIM(brand), '') AS brand,
    NULLIF(TRIM(category_path), '') AS category_path,
    SAFE_CAST(REPLACE(NULLIF(TRIM(site_price_raw), ''), ',', '.') AS NUMERIC) AS site_price,
    SAFE_CAST(REPLACE(NULLIF(TRIM(wholesale_price_1_raw), ''), ',', '.') AS NUMERIC) AS wholesale_price_1,
    NULLIF(TRIM(currency), '') AS currency,
    LOWER(TRIM(displayed_raw)) IN ('да', 'так', 'true', '1', 'yes') AS displayed,
    NULLIF(TRIM(site_availability_raw), '') AS site_availability,
    NULLIF(TRIM(image_main), '') AS image_url,
    NULLIF(TRIM(product_url), '') AS product_url,
    NULLIF(TRIM(product_alias), '') AS product_alias,
    COUNT(*) OVER (PARTITION BY canonical_sku(sku_a)) AS site_source_rows,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_sku(sku_a)
      ORDER BY
        (LOWER(TRIM(displayed_raw)) IN ('да', 'так', 'true', '1', 'yes')) DESC,
        (NULLIF(TRIM(product_url), '') IS NOT NULL) DESC,
        (NULLIF(TRIM(image_main), '') IS NOT NULL) DESC
    ) AS rn
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_horoshop_products`
  WHERE canonical_sku(sku_a) IS NOT NULL
    AND canonical_sku(sku_a) != ''
),
site AS (
  SELECT * EXCEPT(rn)
  FROM site_rows
  WHERE rn = 1
),
cogs_rows AS (
  SELECT
    canonical_sku(sku_raw) AS sku,
    SAFE_CAST(REPLACE(NULLIF(TRIM(final_cogs_raw), ''), ',', '.') AS NUMERIC) AS cogs,
    NULLIF(TRIM(final_source), '') AS cogs_source,
    NULLIF(TRIM(control), '') AS cogs_control,
    COUNT(*) OVER (PARTITION BY canonical_sku(sku_raw)) AS cogs_source_rows,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_sku(sku_raw)
      ORDER BY
        (UPPER(TRIM(control)) = 'OK') DESC,
        SAFE_CAST(REPLACE(NULLIF(TRIM(final_cogs_raw), ''), ',', '.') AS NUMERIC) DESC
    ) AS rn
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_cogs_current`
  WHERE canonical_sku(sku_raw) IS NOT NULL
    AND canonical_sku(sku_raw) != ''
),
cogs AS (
  SELECT * EXCEPT(rn)
  FROM cogs_rows
  WHERE rn = 1
),
stock_rows AS (
  SELECT
    canonical_sku(sku_raw) AS sku,
    SAFE_CAST(REPLACE(NULLIF(TRIM(stock_qty_raw), ''), ',', '.') AS NUMERIC) AS stock_qty,
    COUNT(*) OVER (PARTITION BY canonical_sku(sku_raw)) AS stock_source_rows,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_sku(sku_raw)
      ORDER BY SAFE_CAST(REPLACE(NULLIF(TRIM(stock_qty_raw), ''), ',', '.') AS NUMERIC) DESC
    ) AS rn
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.ext_stock_current`
  WHERE canonical_sku(sku_raw) IS NOT NULL
    AND canonical_sku(sku_raw) != ''
),
stock AS (
  SELECT * EXCEPT(rn)
  FROM stock_rows
  WHERE rn = 1
)
SELECT
  s.sku,
  s.title_ua,
  s.title_en,
  s.brand,
  s.category_path,
  s.site_price,
  s.wholesale_price_1,
  s.currency,
  s.displayed,
  s.site_availability,
  st.stock_qty,
  c.cogs,
  c.cogs_source,
  c.cogs_control,
  s.image_url,
  s.product_url,
  s.product_alias,
  s.site_source_rows,
  COALESCE(c.cogs_source_rows, 0) AS cogs_source_rows,
  COALESCE(st.stock_source_rows, 0) AS stock_source_rows,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM site AS s
LEFT JOIN cogs AS c USING (sku)
LEFT JOIN stock AS st USING (sku);

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_readiness`
CLUSTER BY sku
OPTIONS (
  description = 'ACS Parts SKU readiness v1. No Cooling factual inputs. Current safe policy: stock > 0, valid COGS, image, ACS URL, displayed, and site_price > COGS for paid promotion.'
)
AS
WITH q AS (
  SELECT
    m.*,
    SAFE_DIVIDE(site_price - cogs, site_price) AS site_margin_pct,
    REGEXP_CONTAINS(LOWER(COALESCE(product_url, '')), r'^https?://(www\.)?acsparts\.biz(/|$)') AS domain_ok,
    site_price IS NOT NULL AND site_price > 0 AS price_ok,
    cogs IS NOT NULL AND cogs > 0 AND UPPER(COALESCE(cogs_control, '')) = 'OK' AS cogs_ok,
    image_url IS NOT NULL AND TRIM(image_url) != '' AS image_ok,
    LOWER(TRIM(COALESCE(site_availability, ''))) = 'в наявності' AS site_availability_ok,
    CASE
      WHEN stock_qty IS NULL THEN 'UNKNOWN'
      WHEN stock_qty > 0 THEN 'IN_STOCK'
      ELSE 'ZERO_STOCK'
    END AS stock_status,
    site_source_rows = 1 AS site_source_unique,
    cogs_source_rows <= 1 AS cogs_source_unique,
    stock_source_rows <= 1 AS stock_source_unique
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_master` AS m
)
SELECT
  q.*,
  domain_ok AS url_ok,
  (
    displayed
    AND price_ok
    AND domain_ok
  ) AS sellable,
  (
    displayed
    AND price_ok
    AND domain_ok
    AND cogs_ok
    AND image_ok
    AND site_availability_ok
    AND stock_qty > 0
    AND site_price > cogs
    AND site_source_unique
    AND cogs_source_unique
    AND stock_source_unique
  ) AS promotable,
  ARRAY_TO_STRING(
    ARRAY(
      SELECT reason
      FROM UNNEST([
        IF(NOT displayed, 'HIDDEN', NULL),
        IF(NOT price_ok, 'PRICE', NULL),
        IF(NOT domain_ok, 'URL', NULL),
        IF(NOT cogs_ok, 'COGS', NULL),
        IF(NOT image_ok, 'IMAGE', NULL),
        IF(NOT site_availability_ok, 'SITE_AVAILABILITY', NULL),
        IF(stock_qty IS NULL, 'STOCK_UNKNOWN', NULL),
        IF(stock_qty IS NOT NULL AND stock_qty <= 0, 'STOCK_ZERO', NULL),
        IF(site_price IS NOT NULL AND cogs IS NOT NULL AND site_price <= cogs, 'NEGATIVE_SITE_MARGIN', NULL),
        IF(NOT site_source_unique, 'SITE_SOURCE_DUPLICATE', NULL),
        IF(NOT cogs_source_unique, 'COGS_SOURCE_DUPLICATE', NULL),
        IF(NOT stock_source_unique, 'STOCK_SOURCE_DUPLICATE', NULL)
      ]) AS reason
      WHERE reason IS NOT NULL
    ),
    '|'
  ) AS block_reason,
  'M2_READINESS_V1' AS rule_version,
  CURRENT_TIMESTAMP() AS evaluated_at
FROM q;

-- Read-only preflight output. Review before wiring this mart into any downstream deployment.
SELECT
  COUNT(*) AS sku_total,
  COUNTIF(sellable) AS sellable_sku,
  COUNTIF(promotable) AS promotable_sku,
  COUNTIF(NOT cogs_ok) AS blocked_cogs,
  COUNTIF(stock_status != 'IN_STOCK') AS blocked_stock,
  COUNTIF(NOT domain_ok) AS blocked_domain,
  COUNTIF(site_price IS NOT NULL AND cogs IS NOT NULL AND site_price <= cogs) AS blocked_negative_site_margin,
  COUNTIF(site_source_rows != 1) AS duplicate_site_source,
  COUNTIF(cogs_source_rows > 1) AS duplicate_cogs_source,
  COUNTIF(stock_source_rows > 1) AS duplicate_stock_source
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_readiness`;
