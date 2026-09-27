# M2 — ACS SKU master/readiness migration

This branch implements the approved M2 design without modifying existing GA4 marts or production advertising.

## Why this is separate

The current BigQuery procedure `analytics_demand_acsparts.sp_rebuild_marts` intentionally leaves `purchase_price` and `stock_qty` NULL in GA4 SKU marts. During the audit, using those behavioral marts as product/economic truth was identified as unsafe.

This migration adds a separate canonical product layer:

- Horoshop feed → published ACS product state, site price, URL, availability, image
- БД Цены / COGS итог → canonical current COGS
- Остатки Деловод / Остатки ACS YML → operational stock
- no Cooling data as factual input

## Files

- `migrations/20260927_m2_acs_sku_readiness.sql` — external sources + `mart_sku_master` + `mart_sku_readiness`
- `migrations/20260927_m2_acs_sku_readiness_verify.sql` — audited control-SKU checks and domain guardrail

## Deployment order

1. Run migration in BigQuery EU.
2. Run verification SQL.
3. All four control SKUs must return PASS.
4. `forbidden_domain_rows` must equal 0.
5. Review aggregate blocked/promotable counts.
6. Only after review, wire `mart_sku_readiness` into Search/Merchant routing.
7. Do not alter Ads campaigns or Merchant settings as part of this migration.

## Current safe readiness policy

A SKU may be `sellable` when it is displayed, has a positive site price, and has a valid acsparts.biz URL.

A SKU may be `promotable` only when it also has:

- valid COGS with control = OK
- primary image
- site availability = "В наявності"
- stock > 0
- site price > COGS
- unique source rows

This is deliberately stricter than the future MAKE_TO_ORDER policy. Stock/margin thresholds beyond zero are not introduced until explicitly approved from baseline data.

## Known deployment prerequisite

BigQuery's execution identity must be able to read the three Google Sheets used as external sources. If Drive authorization fails, the migration must stop; do not fall back to Cooling tables or GA4 retail price.
