-- M2 verification: compare canonical readiness against audited control SKUs.
-- Expected from source audit on 2026-09-27:
-- Р04: price 1040, stock 14, COGS ~527.8640, margin ~49.24%, promotable TRUE
-- Р552: price 1560, stock 18, COGS ~1821.8941, margin ~-16.79%, promotable FALSE, NEGATIVE_SITE_MARGIN
-- Р604: price 1560, stock 5, COGS ~1201.7302, margin ~22.97%, promotable TRUE
-- Р874: price 2600, stock 4, no valid COGS, promotable FALSE, COGS

WITH expected AS (
  SELECT 'Р04' AS sku, 1040.0 AS site_price, 14.0 AS stock_qty, 527.8640254 AS cogs, TRUE AS promotable, CAST(NULL AS STRING) AS reason_contains
  UNION ALL
  SELECT 'Р552', 1560.0, 18.0, 1821.894053, FALSE, 'NEGATIVE_SITE_MARGIN'
  UNION ALL
  SELECT 'Р604', 1560.0, 5.0, 1201.730216, TRUE, NULL
  UNION ALL
  SELECT 'Р874', 2600.0, 4.0, NULL, FALSE, 'COGS'
),
actual AS (
  SELECT sku, site_price, stock_qty, cogs, site_margin_pct, promotable, block_reason
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_readiness`
  WHERE sku IN ('Р04','Р552','Р604','Р874')
)
SELECT
  e.sku,
  a.site_price,
  e.site_price AS expected_site_price,
  a.stock_qty,
  e.stock_qty AS expected_stock_qty,
  a.cogs,
  e.cogs AS expected_cogs,
  a.site_margin_pct,
  a.promotable,
  e.promotable AS expected_promotable,
  a.block_reason,
  CASE
    WHEN a.sku IS NULL THEN 'FAIL_MISSING_SKU'
    WHEN ABS(CAST(a.site_price AS FLOAT64) - e.site_price) > 0.01 THEN 'FAIL_PRICE'
    WHEN ABS(CAST(a.stock_qty AS FLOAT64) - e.stock_qty) > 0.01 THEN 'FAIL_STOCK'
    WHEN e.cogs IS NULL AND a.cogs IS NOT NULL THEN 'FAIL_COGS_EXPECTED_NULL'
    WHEN e.cogs IS NOT NULL AND (a.cogs IS NULL OR ABS(CAST(a.cogs AS FLOAT64) - e.cogs) > 0.01) THEN 'FAIL_COGS'
    WHEN a.promotable != e.promotable THEN 'FAIL_PROMOTABLE'
    WHEN e.reason_contains IS NOT NULL AND NOT REGEXP_CONTAINS(COALESCE(a.block_reason, ''), e.reason_contains) THEN 'FAIL_REASON'
    ELSE 'PASS'
  END AS test_status
FROM expected e
LEFT JOIN actual a USING (sku)
ORDER BY e.sku;

-- Scope guardrail: must always return zero.
SELECT COUNT(*) AS forbidden_domain_rows
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.mart_sku_master`
WHERE product_url IS NOT NULL
  AND NOT REGEXP_CONTAINS(LOWER(product_url), r'^https?://(www\.)?acsparts\.biz(/|$)');
