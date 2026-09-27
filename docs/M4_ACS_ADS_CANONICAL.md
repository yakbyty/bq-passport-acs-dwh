# M4 — canonical Google Ads facts for ACS Parts

## Source

Google Ads customer **135-950-6307**, already present in BigQuery dataset `google_ads_acsparts_raw`.

Live validation on 2026-09-27 confirmed `ads_AccountBasicStats_1359506307` contains data through 2026-09-26.

Validated account-day aggregates:

| Date | Spend UAH | Impressions | Clicks |
|---|---:|---:|---:|
| 2026-09-23 | 82.22 | 262 | 11 |
| 2026-09-24 | 46.14 | 234 | 6 |
| 2026-09-25 | 63.04 | 182 | 8 |
| 2026-09-26 | 90.20 | 194 | 12 |

These reconcile with the existing ACS Ads analytics workbook for the same dates.

## Grain choice

Use `AccountBasicStats` and `CampaignBasicStats`, not the generic `*Stats` tables. Generic Stats contain `segments_click_type`; summing impressions across click types double-counts impressions.

BasicStats has no click-type segmentation and its daily aggregate reconciles to the current reporting output.

## Outputs

- `fact_ads_daily`
- `fact_ads_campaign_daily`
- `v_ads_source_health`

All transformations hard-filter customer_id = 1359506307.

## Safety

- no Ads write API
- no budget/bid/tROAS changes
- no campaign status changes
- no Cooling customer allowed
- existing `Аналитика ADS acsparts` remains untouched until canonical facts are deployed and verified

## Deployment acceptance

1. Four dated verification rows PASS.
2. forbidden_customer_rows = 0.
3. Account-day totals reconcile to campaign-day totals.
4. Latest source date is within freshness SLA.
5. Only after that may ACS Tower `09_Реклама_день` and `10_Реклама_кампании` be switched to these marts.
