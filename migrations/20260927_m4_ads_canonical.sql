-- M4: canonical ACS Parts Google Ads facts
-- Project: supple-gearbox-470711-e3
-- Dataset: analytics_demand_acsparts
-- Source account: 135-950-6307 ONLY
-- Safety: read-only transformation from existing Google Ads transfer.
-- No Google Ads writes. No campaign/budget/bid changes.

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily`
PARTITION BY date
OPTIONS (
  description = 'ACS Parts Google Ads daily facts from AccountBasicStats. Account 135-950-6307 only.'
)
AS
SELECT
  segments_date AS date,
  customer_id,
  SUM(metrics_cost_micros) / 1000000.0 AS spend,
  SUM(metrics_impressions) AS impressions,
  SUM(metrics_clicks) AS clicks,
  SUM(metrics_conversions) AS ads_conversions,
  SUM(metrics_conversions_value) AS ads_conversion_value,
  SAFE_DIVIDE(SUM(metrics_clicks), SUM(metrics_impressions)) AS ctr,
  SAFE_DIVIDE(SUM(metrics_cost_micros) / 1000000.0, SUM(metrics_clicks)) AS cpc,
  SAFE_DIVIDE(SUM(metrics_conversions), SUM(metrics_clicks)) AS ads_cr,
  SAFE_DIVIDE(SUM(metrics_conversions_value), SUM(metrics_cost_micros) / 1000000.0) AS ads_roas,
  MAX(_DATA_DATE) AS source_data_date,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `supple-gearbox-470711-e3.google_ads_acsparts_raw.ads_AccountBasicStats_1359506307`
WHERE customer_id = 1359506307
  AND _DATA_DATE = segments_date
GROUP BY date, customer_id;

CREATE OR REPLACE TABLE `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_campaign_daily`
PARTITION BY date
CLUSTER BY campaign_id
OPTIONS (
  description = 'ACS Parts Google Ads campaign-day facts from CampaignBasicStats. Account 135-950-6307 only.'
)
AS
WITH stats AS (
  SELECT
    segments_date AS date,
    customer_id,
    campaign_id,
    SUM(metrics_cost_micros) / 1000000.0 AS spend,
    SUM(metrics_impressions) AS impressions,
    SUM(metrics_clicks) AS clicks,
    SUM(metrics_conversions) AS ads_conversions,
    SUM(metrics_conversions_value) AS ads_conversion_value,
    MAX(_DATA_DATE) AS source_data_date
  FROM `supple-gearbox-470711-e3.google_ads_acsparts_raw.ads_CampaignBasicStats_1359506307`
  WHERE customer_id = 1359506307
    AND _DATA_DATE = segments_date
  GROUP BY date, customer_id, campaign_id
),
campaign_meta AS (
  SELECT
    campaign_id,
    customer_id,
    campaign_name,
    campaign_status,
    campaign_serving_status,
    campaign_advertising_channel_type,
    campaign_advertising_channel_sub_type,
    campaign_bidding_strategy_type,
    campaign_budget_amount_micros / 1000000.0 AS campaign_budget_daily,
    campaign_maximize_conversion_value_target_roas AS target_roas,
    _DATA_DATE AS metadata_data_date
  FROM `supple-gearbox-470711-e3.google_ads_acsparts_raw.ads_Campaign_1359506307`
  WHERE customer_id = 1359506307
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, campaign_id
    ORDER BY _DATA_DATE DESC
  ) = 1
)
SELECT
  s.date,
  s.customer_id,
  s.campaign_id,
  m.campaign_name,
  m.campaign_status,
  m.campaign_serving_status,
  m.campaign_advertising_channel_type AS campaign_type,
  m.campaign_advertising_channel_sub_type AS campaign_sub_type,
  m.campaign_bidding_strategy_type AS bidding_strategy_type,
  m.campaign_budget_daily,
  m.target_roas,
  s.spend,
  s.impressions,
  s.clicks,
  s.ads_conversions,
  s.ads_conversion_value,
  SAFE_DIVIDE(s.clicks, s.impressions) AS ctr,
  SAFE_DIVIDE(s.spend, s.clicks) AS cpc,
  SAFE_DIVIDE(s.ads_conversions, s.clicks) AS ads_cr,
  SAFE_DIVIDE(s.ads_conversion_value, s.spend) AS ads_roas,
  s.source_data_date,
  m.metadata_data_date,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM stats s
LEFT JOIN campaign_meta m
  USING (customer_id, campaign_id);

CREATE OR REPLACE VIEW `supple-gearbox-470711-e3.analytics_demand_acsparts.v_ads_source_health`
OPTIONS (
  description = 'ACS Parts Ads source health and hard account guardrail.'
)
AS
SELECT
  'GOOGLE_ADS' AS source_id,
  1359506307 AS expected_customer_id,
  MAX(date) AS last_data_date,
  DATE_DIFF(CURRENT_DATE('Europe/Kyiv'), MAX(date), DAY) AS lag_days,
  COUNTIF(customer_id != 1359506307) AS forbidden_customer_rows,
  CASE
    WHEN COUNTIF(customer_id != 1359506307) > 0 THEN 'FAILED_SCOPE'
    WHEN DATE_DIFF(CURRENT_DATE('Europe/Kyiv'), MAX(date), DAY) <= 1 THEN 'FRESH'
    WHEN DATE_DIFF(CURRENT_DATE('Europe/Kyiv'), MAX(date), DAY) <= 2 THEN 'LAGGING'
    ELSE 'STALE'
  END AS health_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily`;

-- Preflight output: daily aggregate must reconcile to campaign aggregate.
SELECT
  a.date,
  a.spend AS account_spend,
  c.spend AS campaign_spend,
  a.impressions AS account_impressions,
  c.impressions AS campaign_impressions,
  a.clicks AS account_clicks,
  c.clicks AS campaign_clicks,
  ABS(a.spend - c.spend) AS spend_diff
FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_daily` a
LEFT JOIN (
  SELECT
    date,
    SUM(spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
  FROM `supple-gearbox-470711-e3.analytics_demand_acsparts.fact_ads_campaign_daily`
  GROUP BY date
) c USING (date)
ORDER BY a.date DESC
LIMIT 30;
