# 📦 Supply Chain Data Warehouse — Data Dictionary
**Project:** `supply_text` | **Schema:** Star Schema | **DB:** SQL Server | 
**Date Range:** 2020-01-01 → 2025-12-31 | **Market:** Canada (CAD)

---

## 📌 Table of Contents
1. [Schema Overview](#schema-overview)
2. [fact_order_details](#fact_order_details)
3. [fact_payments](#fact_payments)
4. [fact_deliveries](#fact_deliveries)
5. [dim_retailers](#dim_retailers)
6. [dim_suppliers](#dim_suppliers)
7. [dim_products](#dim_products)
8. [dim_areas](#dim_areas)
9. [dim_drivers](#dim_drivers)
10. [dim_date](#dim_date)
11. [Status Dictionary](#status-dictionary)
12. [Anomaly Combinations](#anomaly-combinations)

---

## Schema Overview

| Table | Type | PK | Rows (approx) | Description |
|---|---|---|---|---|
| `fact_order_details` | Fact | `detail_id` | ~196K | Core transaction grain — one row per order line |
| `fact_payments` | Fact | `payment_id` | ~81.8K | Payment events per order |
| `fact_deliveries` | Fact | `delivery_id` | ~81.8K | Delivery tracking events |
| `dim_retailers` | Dimension | `retailer_id` | 500 | Retailer master — 18 Canadian cities |
| `dim_suppliers` | Dimension | `supplier_id` | 80 | Supplier master — 10 product categories |
| `dim_products` | Dimension | `product_id` | 160 | Product catalogue — 12 categories |
| `dim_areas` | Dimension | `area_id` | 108 | Geographic zones — 8 provinces |
| `dim_drivers` | Dimension | `driver_id` | 120 | Driver master — 5 vehicle types |
| `dim_date` | Dimension | `date` | 2,258 | Date spine 2020-01-01 → 2026-03-07 |

---

## fact_order_details

> One row per order line item. Central fact table linking all 6 dimensions.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `detail_id` | int | PK | No | Unique order line identifier | Sequential |
| `order_id` | int | | No | Groups multiple lines per order | Multiple lines share same ID |
| `product_id` | int | FK | No | → `dim_products.product_id` | |
| `retailer_id` | int | FK | No | → `dim_retailers.retailer_id` | |
| `supplier_id` | int | FK | No | → `dim_suppliers.supplier_id` | |
| `area_id` | int | FK | No | → `dim_areas.area_id` | |
| `driver_id` | int | FK | Yes | → `dim_drivers.driver_id` — NULL if unassigned | |
| `order_date` | date | FK | No | → `dim_date.date` | 2020-01-01 → 2025-12-31 |
| `order_status` | string | | No | Current status of the order | See [Status Dictionary](#order_status) |
| `quantity` | int | | No | Units ordered per line | 1 – 500 · avg: 21 |
| `unit_price` | decimal | | No | Price per unit at order time (CAD) | 2.83 – 404.10 · avg: 41.44 |
| `line_total` | decimal | | No | `quantity × unit_price` — computed in ETL | 2.89 – 42,418.88 |

---

## fact_payments

> One payment event per order. Currency is always CAD.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `payment_id` | int | PK | No | Unique payment identifier | Sequential |
| `order_id` | int | | No | Links to order | Cross-ref `fact_order_details.order_id` |
| `retailer_id` | int | FK | No | → `dim_retailers.retailer_id` | |
| `payment_method` | string | | No | How payment was made | See [Status Dictionary](#payment_method) |
| `payment_status` | string | | No | Current payment state | See [Status Dictionary](#payment_status) |
| `amount` | decimal | | No | Amount paid / attempted (CAD) | 2.89 – 44,485.86 · avg: 1,364.43 |
| `payment_date` | date | FK | No | → `dim_date.date` | |
| `currency` | string | | No | ISO 4217 currency code | `CAD` (only value in dataset) |

---

## fact_deliveries

> One delivery record per order. Tracks scheduled vs actual delivery with delay metrics.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `delivery_id` | int | PK | No | Unique delivery identifier | Sequential |
| `order_id` | int | | No | Links to order | Cross-ref `fact_order_details.order_id` |
| `driver_id` | int | FK | Yes | → `dim_drivers.driver_id` | NULL if not dispatched |
| `area_id` | int | FK | No | → `dim_areas.area_id` | |
| `scheduled_date` | date | FK | No | → `dim_date.date` — promised date | |
| `scheduled_at` | datetime | | Yes | Exact scheduled slot datetime | NULL until slot booked |
| `actual_at` | datetime | | Yes | Datetime delivery occurred | NULL if not yet delivered |
| `delivery_status` | string | | No | Current delivery state | See [Status Dictionary](#delivery_status) |
| `delay_hours` | decimal | | Yes | Hours late vs scheduled (negative = early) | -10.75 – 130.25 · avg: 12.0 |
| `extra_delay_days` | int | | Yes | Days beyond promised window | 0 – 5 |
| `is_cold_city` | bit | | No | 1 if area requires cold-chain logistics | 73,125 of 81,837 records = 89% |
| `is_winter_month` | bit | | No | 1 if delivery month is Nov/Dec/Jan/Feb | 25,705 of 81,837 records = 31% |

---

## dim_retailers

> Retailer master data. 500 retailers across 18 Canadian cities. Segments are RFM-based.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `retailer_id` | int | PK | No | Unique retailer identifier | |
| `retailer_name` | string | | No | Legal trading name | |
| `segment` | string | | No | RFM-based customer segment | `Potential Loyalists` (83) · `Loyal Customers` (75) · `New Customers` (74) · `At Risk` (70) · `Lost Customers` (69) · `Champions` (65) · `Hibernating` (64) |
| `city` | string | | No | Retailer city | Toronto (97) · Vancouver (64) · Montreal (57) · Edmonton (44) · Calgary (38) · + 13 others |
| `province` | string | | No | Canadian province abbreviation | ON · QC · AB · BC · SK · MB · NS · NL |
| `cohort_year` | smallint | | Yes | Year retailer first registered | 2019 – 2025 |
| `preferred_payment` | string | | Yes | Preferred payment method | `Credit Card` (224) · `Net-30` (116) · `Net-60` (68) · `Bank Transfer` (64) · `Cash on Delivery` (28) |
| `registration_date` | date | | No | Onboarding date | Cannot be future date |

---

## dim_suppliers

> Supplier master. 80 suppliers covering 10 primary categories. All rated 3.0–5.0.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `supplier_id` | int | PK | No | Unique supplier identifier | |
| `supplier_name` | string | | No | Supplier company name | |
| `supplier_rating` | decimal | | Yes | Performance rating | 3.00 – 5.00 · avg: 4.08 |
| `city` | string | | No | Supplier HQ city | Canadian cities |
| `province` | string | | No | Supplier HQ province | Canadian provinces |
| `primary_category` | string | | No | Main product category | `Dairy` (16) · `Frozen Foods` (12) · `Beverages` (12) · `Packaging Materials` (8) · `Grains & Rice` (7) · `Health & Personal Care` (5) · `Industrial Supplies` (5) · `Office Supplies` (5) · `Produce` (5) · `Cleaning Supplies` (5) |
| `category_group` | string | | No | Pipe-separated multi-category group | e.g. `Frozen Foods\|Dairy\|Produce` |
| `established_year` | smallint | | Yes | Year supplier was established | 1985 – 2019 |
| `spec_group_id` | smallint | | Yes | Internal specialisation group ID | References procurement system |

---

## dim_products

> Product catalogue. 160 products across 12 categories.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `product_id` | int | PK | No | Unique product identifier | |
| `product_name` | string | | No | Full product display name | |
| `category` | string | | No | Product category | `Dairy` (20) · `Beverages` (16) · `Cleaning Supplies` (16) · `Grains & Rice` (16) · `Produce` (16) · `Snacks & Confectionery` (14) · `Health & Personal Care` (14) · `Meat & Seafood` (14) · `Office Supplies` (14) · `Packaging Materials` (14) · `Frozen Foods` (14) · `Industrial Supplies` (12) |
| `sku` | string | | No | Unique stock-keeping unit code | Must be unique across all products |
| `unit_price` | decimal | | No | Standard catalogue price (CAD) | 3.14 – 367.37 · avg: 40.26 |

---

## dim_areas

> Geographic delivery zones. 108 zones across 8 Canadian provinces. 42 are cold-chain areas.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `area_id` | int | PK | No | Unique area identifier | |
| `city` | string | | No | City of the area | 18 cities · 6 zones per city |
| `province` | string | | No | Canadian province abbreviation | ON (42) · QC · SK · AB · BC (12 each) · MB · NL · NS (6 each) |
| `neighborhood` | string | | Yes | Specific district or neighbourhood | Nullable for city-level zones |
| `area_name` | string | | No | Human-readable zone label | |
| `is_cold` | bit | | No | 1 if area requires cold-chain logistics | 42 of 108 areas = 39% |
| `city_pop_weight` | decimal | | Yes | Relative population weight | 0.01 – 0.18 · sums to 1.0 across all areas |

---

## dim_drivers

> Driver master. 120 drivers, 110 currently active. 5 vehicle types.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `driver_id` | int | PK | No | Unique driver identifier | |
| `driver_name` | string | | No | Driver full name | |
| `vehicle_type` | string | | No | Vehicle type assigned | `Cargo Van` (49) · `Box Truck` (42) · `Refrigerated Truck` (16) · `Pickup Truck` (8) · `Electric Van` (5) |
| `driver_rating` | decimal | | Yes | Performance rating | 3.20 – 5.00 · avg: 4.08 · NULL if new driver |
| `city` | string | | No | Driver base city | Canadian cities |
| `province` | string | | No | Driver base province | Canadian provinces |
| `hire_year` | smallint | | No | Year driver was hired | 2015 – 2025 |
| `active` | bit | | No | 1 if currently active | 110 active / 10 inactive |
| `primary_area_id` | tinyint | | Yes | → `dim_areas.area_id` — home zone | Not enforced by FK constraint |

---

## dim_date

> Continuous date spine. 2,258 days from 2020-01-01 to 2026-03-07.

| Column | Type | Key | Nullable | Description | Actual Values |
|---|---|---|---|---|---|
| `date` | date | PK | No | Calendar date | 2020-01-01 → 2026-03-07 |
| `date_key` | int | | No | Integer surrogate: YYYYMMDD | 20200101 … |
| `year` | int | | No | Calendar year | 2020 – 2026 |
| `month` | int | | No | Month number | 1 (Jan) – 12 (Dec) |
| `month_name` | string | | No | Full English month name | January … December |
| `quarter` | int | | No | Quarter number | 1 – 4 |
| `day` | int | | No | Day of month | 1 – 31 |
| `is_weekend` | bit | | No | 1 if Saturday or Sunday | 645 of 2,258 days = 29% |
| `is_winter` | bit | | No | 1 if month in Nov / Dec / Jan / Feb | 601 of 2,258 days = 27% |

---

## Status Dictionary

### order_status

| Value | Count | % | Description | Terminal? | Valid Transitions |
|---|---|---|---|---|---|
| `Completed` | 173,266 | 88.6% | Order fully processed and closed | ✅ Yes | — |
| `Pending` | 13,804 | 7.1% | Order placed, awaiting processing | No | → Completed, Cancelled |
| `Cancelled` | 9,511 | 4.9% | Order cancelled before fulfilment | ✅ Yes | — |

> ⚠️ **Note:** No `Shipped` or `Confirmed` status exists in actual data. Only 3 values observed.

---

### delivery_status

| Value | Count | % | Description | Terminal? |
|---|---|---|---|---|
| `Delivered` | 66,628 | 81.4% | Successfully handed to retailer | ✅ Yes |
| `Delayed` | 8,057 | 9.8% | In transit but past scheduled date | No |
| `Not Dispat` | 3,995 | 4.9% | Not dispatched (truncated: "Not Dispatched") | No |
| `Failed` | 2,381 | 2.9% | Delivery attempt failed | No |
| `In Transit` | 776 | 0.9% | Currently en route | No |

> ⚠️ **Note:** `Not Dispat` appears truncated in the source system — likely `Not Dispatched`. Correlates 1:1 with `Cancelled` orders (9,511 ≈ 3,995 after join). Verify column length constraint in source.

---

### payment_status

| Value | Count | % | Description | Terminal? |
|---|---|---|---|---|
| `Paid` | 71,538 | 87.4% | Payment successfully processed | ✅ Yes |
| `Pending` | 4,149 | 5.1% | Initiated, awaiting processing | No |
| `Refunded` | 3,995 | 4.9% | Amount returned to retailer | ✅ Yes |
| `Failed` | 2,155 | 2.6% | Payment attempt rejected | No |

> ⚠️ **Note:** The value `Paid` is used here (not `Completed` as designed). Update ETL or documentation to reflect actual value.

---

### payment_method

| Value | Count | % |
|---|---|---|
| `Credit Card` | 39,970 | 48.8% |
| `Net-60` | 15,135 | 18.5% |
| `Net-30` | 12,582 | 15.4% |
| `Bank Transfer` | 10,953 | 13.4% |
| `Cash on Delivery` | 3,197 | 3.9% |

---

## Anomaly Combinations

> Cross-status combinations observed in actual data. Sorted by count descending.

### ✅ Normal Combinations (expected business logic)

| order_status | delivery_status | payment_status | Count | Notes |
|---|---|---|---|---|
| `Completed` | `Delivered` | `Paid` | 136,250 | ✅ Healthy — standard fulfilled order |
| `Completed` | `Delayed` | `Paid` | 16,439 | ✅ Acceptable — late but paid |
| `Cancelled` | `Not Dispat` | `Refunded` | 9,511 | ✅ Expected cancellation flow |
| `Completed` | `In Transit` | `Paid` | 1,514 | ✅ Likely status sync lag |

---

### ⚠️ Anomalous Combinations (require investigation)

| Severity | order_status | delivery_status | payment_status | Count | Issue |
|---|---|---|---|---|---|
| 🔴 Critical | `Pending` | `Delivered` | `Paid` | 10,804 | Order never confirmed but physically delivered and paid |
| 🟡 High | `Completed` | `Delivered` | `Pending` | 7,950 | Order complete, delivered, but payment still pending |
| 🟠 Medium | `Completed` | `Failed` | `Paid` | 5,010 | Delivery failed but payment collected — no goods delivered |
| 🔴 Critical | `Completed` | `Delivered` | `Failed` | 4,072 | Delivered and completed but payment failed — revenue gap |
| 🟡 High | `Pending` | `Delayed` | `Paid` | 1,288 | Paid for a pending order that is already delayed |
| 🟠 Medium | `Completed` | `Delayed` | `Pending` | 970 | Completed order with delay but payment not settled |
| 🟠 Medium | `Pending` | `Failed` | `Paid` | 380 | Delivery failed on a pending order that was paid |
| 🟡 High | `Pending` | `Delivered` | `Pending` | 711 | Delivered but order still pending and payment not settled |
| 🔴 Critical | `Pending` | `Delivered` | `Failed` | 315 | Delivered with no valid order confirmation and failed payment |
| 🟠 Medium | `Completed` | `Failed` | `Pending` | 284 | Failed delivery, order completed, payment not settled |
| 🟠 Medium | `Completed` | `Delayed` | `Failed` | 499 | Completed order, delayed delivery, payment failed |
| 🔴 Critical | `Completed` | `Failed` | `Failed` | 125 | Fully completed order with both delivery and payment failures |
| 🟠 Medium | `Pending` | `Delayed` | `Pending` | 82 | Stalled order — delayed and unpaid |
| 🟠 Medium | `Completed` | `In Transit` | `Failed` | 54 | Marked complete but still in transit with failed payment |
| 🟡 High | `Completed` | `In Transit` | `Pending` | 99 | Marked complete but still in transit |
| 🟠 Medium | `Pending` | `Delayed` | `Failed` | 28 | Delayed, pending, and payment failed |
| 🟠 Medium | `Pending` | `Failed` | `Failed` | 26 | Double failure on pending order |
| 🟠 Medium | `Pending` | `Failed` | `Pending` | 24 | Failed delivery on unconfirmed, unpaid order |
| 🟠 Medium | `Pending` | `In Transit` | `Pending` | 7 | In transit but order and payment both pending |
| 🔴 Critical | `Pending` | `In Transit` | `Failed` | 4 | In transit with unconfirmed order and failed payment |

---

### Anomaly Summary by Severity

| Severity | Combinations | Total Records |
|---|---|---|
| 🔴 Critical | 5 | 15,320 |
| 🟡 High | 5 | 11,038 |
| 🟠 Medium | 10 | 2,548 |
| **Total anomalous** | **20** | **28,906** |

> **28,906 records (~14.8% of total)** have cross-status inconsistencies and require data quality review or ETL correction.

---
