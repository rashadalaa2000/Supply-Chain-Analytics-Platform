# SQL — CanRoute Data Warehouse

> Technical documentation for this folder. For the full project walkthrough (insights, business problems, and recommendations), see the [root README](../README.md).

---

## Contents

```
📂sql
 ┣ 📂analysis                 → business-question queries answered directly in SQL
 ┃ ┗ 📂views                  → reusable analytical views
 ┗ 📂dw                       → data warehouse DDL and ETL scripts
```

---

## 1. Data Warehouse (`sql/dw/`)

Run in this order — each step depends on the one before it:

| Order | File | Purpose |
|---|---|---|
| 1 | `01_load_staging.sql` | Loads the raw CSVs into `source_*` staging tables via `BULK INSERT`. |
| 2 | `02_create_schema.sql` | DDL for the galaxy schema (fact constellation): 2 fact tables + 6 dimension tables. Includes `dim_date`, generated as a continuous calendar via a recursive CTE (rather than a hardcoded date list). |
| 3 | `03_load_warehouse.sql` | ETL that transforms the `source_*` staging tables into the final `dim_*` / `fact_*` warehouse. |

### Design Notes

**Grain-based fact consolidation.** The data model originally had four candidate fact sources — **order**, **payment**, **delivery**, and **order details**. Order, payment, and delivery all shared the same grain (one row per order), so rather than keeping them as three separate fact tables (which would force fan-out joins at query time), they were consolidated into a single `fact_orders` table. `fact_order_details` was kept separate since it sits at a different, lower grain (one row per order line/product).

**Fact tables**
- `fact_orders` — one row per order: order status, plus the payment attributes (`payment_status`, `payment_method`, `payment_amount`, `payment_date`) and delivery attributes (`delivery_status`, `scheduled_at`, `actual_at`, `delay_hours`, etc.) for that order.
- `fact_order_details` — one row per order line (product, quantity, unit price, line total).

**Dimension tables** — `dim_date`, `dim_areas`, `dim_retailers`, `dim_suppliers`, `dim_products`, `dim_drivers`. See [`docs/data-model/`](../docs/data-model) for the full diagram.

**Naming note.** The project itself is named **CanRoute** (Supply-Chain-Analytics-Platform), but the underlying SQL Server database is named `supply_chain` — `02_create_schema.sql` and `03_load_warehouse.sql` both target `supply_chain`.

**Known issues resolved during ETL build:** `BULK INSERT` path/permission errors during `03_load_warehouse.sql` development were debugged and resolved; see the design notes above for the fact-table consolidation decision.

---

## 2. Analysis Queries (`sql/analysis/`)

Each script answers a specific business question directly in SQL, independent of the Power BI model — these were used to validate metrics and explore the data before building the corresponding DAX measures. All queries run against the consolidated `fact_orders` / `fact_order_details` model above.

| File | Business Question It Answers |
|---|---|
| `eda.sql` | General exploratory data profiling — row counts across every dimension, total/average order value, and cash collected vs. outstanding. |
| `order_management.sql` | Order lifecycle and status-combination analysis — how orders move through order/delivery/payment statuses, cancellation rate, repeat-order rate, and order value trends over time. |
| `delivery_and_logistics.sql` | Delivery timeliness, delay-status distribution, driver productivity, and cold-chain compliance by geography/season. |
| `financial_health.sql` | Cash collection, accrued vs. at-risk revenue, refund cost classification, and an overall revenue-at-risk percentage. |
| `retailer_analysis.sql` | Retailer-level performance — average retailer lifetime, month-over-month retention, RFM segmentation, churn (no orders in 365 days), and cohort conversion. |
| `product_and_supplier_performance.sql` | Product-category revenue, top products/suppliers by revenue, supplier fill rate, and market-basket product pairs (frequency > 20). |
| `paid_orders_performance_analysis.sql` | Revenue, order count, and revenue share broken down by retailer, area, and supplier — scoped only to orders with a confirmed `Paid` payment status. |
| `supply_chain_health_check.sql` | Cross-cutting classification of every order/delivery/payment status combination into a business-impact label (e.g. "Critical Loss", "Logistics Disaster", "Safe Cancellation") — the basis for the anomaly detection later surfaced as `Order_Anomaly_Flag` in the Power BI model (~16.5% of orders show cross-status anomalies). |

---

## 3. Analytical Views (`sql/analysis/views/`)

| View | Purpose |
|---|---|
| `vw_market_basket.sql` | Supports product-affinity / market-basket analysis (which products are frequently ordered together), based on the same pairwise co-occurrence logic as the query in `product_and_supplier_performance.sql`. |
| `vw_supply_chain_health.sql` | Same order/delivery/payment status classification as `supply_chain_health_check.sql`, packaged as a reusable view for Power BI and ad-hoc queries. Reads directly from `fact_orders`, and orders its `CASE` branches so that cancelled orders and the "status out of sync" anomaly are always caught before the general Delivered/Paid rules — otherwise those branches would be unreachable. |

---

## Related Documentation

- [Root README](../README.md) — full project walkthrough (insights, problems, recommendations)
- [Power BI README](../power_bi/README.md) — dashboard file, pages, and DAX documentation
- [`docs/data-model/`](../docs/data-model) — conceptual model, galaxy schema diagram, data dictionary, and Power BI model view
- [`docs/reports/`](../docs/reports) — dashboard insights, business problems, and business recommendations reports