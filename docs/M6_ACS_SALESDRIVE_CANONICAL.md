# M6 — SalesDrive canonical integration for ACS Parts

## Current finding

The SalesDrive pipeline already exists in project `supple-gearbox-470711-e3`:

- `salesdrive_raw.orders_raw`
- `salesdrive_raw.payments_raw`
- `salesdrive_raw.load_state`
- `salesdrive_core.v_orders`
- `salesdrive_core.v_order_items`
- `salesdrive_core.dim_order_statuses`
- `salesdrive_dwh.fact_orders`
- `salesdrive_dwh.fact_payments`

The current ACS DWH Passport snapshot classifies these objects as **LEGACY_UNUSED_OR_UNVERIFIED** and shows zero jobs in the last 30 days.

The passport object metadata also shows the raw order table last modified on 2025-12-23 and payment/check data around 2025-12-24. Therefore M6 must NOT treat the existing pipeline as a current order source until live preflight proves otherwise.

## Existing schema that can be reused

`salesdrive_core.v_orders` already exposes:

- order_id
- form_id
- status_id
- organization_id
- manager_user_id
- order_time / order_updated_at
- payment_amount / payed_amount / profit_amount
- payment/shipping identifiers
- contact identifiers
- delivery/tracking fields

`salesdrive_core.v_order_items` already exposes:

- order_id
- product_id
- sku
- product_name
- manufacturer
- quantity
- price
- cost_price
- discount / commission

`dim_order_statuses` contains the known lifecycle statuses, including Новий, В обработке, Підтверджено, Ждет комплектации, Відправлено, Оплачено, Продано, Відмова and others.

## Gate before canonical ACS facts

Run `migrations/20260928_m6_salesdrive_preflight.sql`.

M6 deployment is blocked until all of these are proven:

1. loader/source freshness is current;
2. the exact ACS Parts scope is identified (form_id / organization_id / site or another deterministic field);
3. no Cooling/non-ACS orders enter the ACS fact;
4. SKU completeness is acceptable;
5. actual payload fields for UTM / click IDs are verified;
6. order status semantics are confirmed against the current SalesDrive configuration.

## Target outputs after the gate passes

- `analytics_demand_acsparts.fact_salesdrive_orders`
- `analytics_demand_acsparts.fact_salesdrive_order_items`
- `analytics_demand_acsparts.fact_salesdrive_status_lifecycle` if status history is available
- `analytics_demand_acsparts.v_salesdrive_source_health`

These will represent **incoming order lifecycle**, not the financial sale SoT. Shipped/completed revenue remains reconciled against `Заказы 2026`.

## Safety

- no SalesDrive writes;
- no CRM status changes;
- no Ads changes;
- no Merchant changes;
- no use for automated decisions while freshness/scope gate is failing;
- do not infer ACS scope from free-text source/channel labels.

## Next implementation step

Restore or verify the SalesDrive loader first. After fresh data is available, run the preflight, freeze the exact ACS scope rule and attribution mapping, then create the canonical facts and reconciliation to shipped orders.
