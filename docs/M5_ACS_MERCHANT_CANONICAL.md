# M5 — canonical Merchant Center current state

Merchant account: **172724510**.

## Audit correction

The previous Passport status `READY_FOR_AUTH / NOT_CONNECTED_TO_DWH` was stale.

On 2026-09-27 the DWH inventory contains:

- `merchant_center_acsparts_raw.Products_172724510` — 2554 raw partitioned rows, refreshed 2026-09-27
- `merchant_center_acsparts_raw.ProductTargeting_172724510` — 5700 rows, refreshed 2026-09-27
- `merchant_center_acsparts_raw.ProductPerformance_172724510` — 0 rows

The live Merchant connector also confirms account 172724510.

## Important QA gap

The Google Sheet `фід GMC – ENG.biz` contains **5846 unique non-empty IDs**.
Its formula/output source contains **4555 unique non-empty IDs**.

An audited comparison shows:

- R04 exists in the GMC formula layer
- R04 has stock, site URL, photo, and `отправлять в мерчант центр = TRUE`
- R04 exists in the ENG output feed
- exact live Merchant product_id filter for R04 returns 0 rows
- R290 has equivalent source readiness and exact live Merchant filter returns product resources

Therefore Merchant ingestion/processing coverage must be reconciled before Merchant status is allowed to control routing.

The generic live product query returns a limited page, so its total row count must NOT be treated as total Merchant catalog size. Exact server-side SKU filters are used only as point checks.

## Migration outputs

- `mart_merchant_products_current` — latest partition only; one row per Merchant product resource
- `fact_merchant_destinations_current` — exploded destination states
- `fact_merchant_issues_current` — exploded issue records
- `mart_merchant_offer_current` — canonical SKU/offer summary across locales
- `v_merchant_source_health`

## Why latest partition only

Merchant Products is a partitioned snapshot-style transfer. Selecting the latest version ever seen per product can incorrectly keep products that disappeared later. M5 uses only the latest source partition, then deduplicates product_id inside that partition.

## Acceptance before routing

1. forbidden_merchant_rows = 0
2. duplicate product IDs = 0
3. source health fresh
4. current product/resource counts reviewed
5. R290/R04 discrepancy explained
6. major issue and destination classes reviewed
7. only after M2 is deployed: compare Merchant canonical_sku coverage to `mart_sku_readiness`
8. do not auto-exclude products or edit feeds during M5
