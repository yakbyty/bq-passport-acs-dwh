-- M4 verification for ACS Parts Google Ads canonical facts.
-- Must be run after 20260927_m4_ads_canonical.sql.
-- No writes.

WITH expected AS (
  SELECT DATE '2026-09-23' AS date, 82.22 AS spend, 262 AS impressions, 11 AS clicks
  UNION ALL SELECT DATE '2026-09-24', 46.14, 234, 6
  UNION ALL SELECT DATE '2026-09-25', 63.04, 182, 8
  UNION ALL SELECT DATE '2026-09-26', 90.20, 194, 12
),
actual AS (
  SELECT date, spend, impressions, clicks, customer_id
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily`
  WHERE date BETWEEN DATE '2026-09-23' AND DATE '2026-09-26'
)
SELECT
  e.date,
  a.spend,
  e.spend AS expected_spend,
  a.impressions,
  e.impressions AS expected_impressions,
  a.clicks,
  e.clicks AS expected_clicks,
  a.customer_id,
  CASE
    WHEN a.date IS NULL THEN 'FAIL_MISSING_DAY'
    WHEN a.customer_id != 1359506307 THEN 'FAIL_CUSTOMER'
    WHEN ABS(a.spend - e.spend) > 0.01 THEN 'FAIL_SPEND'
    WHEN a.impressions != e.impressions THEN 'FAIL_IMPRESSIONS'
    WHEN a.clicks != e.clicks THEN 'FAIL_CLICKS'
    ELSE 'PASS'
  END AS test_status
FROM expected e
LEFT JOIN actual a USING (date)
ORDER BY e.date;

-- Hard account isolation. Must return zero.
SELECT COUNT(*) AS forbidden_customer_rows
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily`
WHERE customer_id != 1359506307;

-- Account and campaign daily grain must reconcile.
SELECT
  a.date,
  ROUND(a.spend, 2) AS account_spend,
  ROUND(c.spend, 2) AS campaign_spend,
  a.impressions AS account_impressions,
  c.impressions AS campaign_impressions,
  a.clicks AS account_clicks,
  c.clicks AS campaign_clicks,
  CASE
    WHEN ABS(a.spend - c.spend) <= 0.01
     AND a.impressions = c.impressions
     AND a.clicks = c.clicks
    THEN 'PASS'
    ELSE 'FAIL'
  END AS reconciliation_status
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily` a
JOIN (
  SELECT
    date,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_campaign_daily`
  GROUP BY date
) c USING (date)
ORDER BY a.date DESC;
