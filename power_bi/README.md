# Power BI — CanRoute Dashboard

> Technical documentation for this folder. For the full project walkthrough (insights, business problems, and recommendations), see the [root README](../README.md).

---

## Contents

```
📂power_bi
 ┣ 📂screenshots             → PNG exports of every page/visual, used in the root README
 ┃ ┣ 📂1-executive_overview
 ┃ ┣ 📂2-logistics_and_delivery
 ┃ ┣ 📂3-retailer_performance
 ┃ ┗ 📂4-financial_health
 ┗ dashboard.pbix            → the Power BI report file
```

---

## Requirements

- **Power BI Desktop** (latest version recommended) — that's it.
- Data is loaded in **Import mode**: the `.pbix` file already contains the full dataset, so it opens and works standalone with no database setup, connection, or credentials needed.

---

## Opening the Report

1. Open `dashboard.pbix` in Power BI Desktop.
2. Browse the 4 pages directly — everything is self-contained.

---

## Report Pages

| Page | Description |
|---|---|
| **1. Executive Overview** | Top-line KPIs (Realized Revenue, Collection Rate, Mismatch Rate), revenue trend, revenue by region/category/payment method, and ABC supplier concentration. |
| **2. Logistics & Delivery Operations** | Delay Rate, on-time performance by month, delay-status distribution, cash-at-risk by province, weather impact, and cross-status anomaly severity. |
| **3. Retailer Performance** | Retailer-level KPIs (AOV, Order Frequency, ARPR), RFM segmentation, Pareto analysis by segment, YoY retailer growth, and cohort retention matrix. |
| **4. Financial Health** | Cash Collected, Outstanding Receivables, Anomaly Cash Exposure, the cash reconciliation waterfall, GMV by operational status, and AR aging. |

---

## Data Model

The report is built on a **galaxy schema** (fact constellation) with two fact tables (`fact_orders`, `fact_order_details`) sharing six dimension tables (`dim_date`, `dim_areas`, `dim_retailers`, `dim_suppliers`, `dim_products`, `dim_drivers`). See [`docs/data-model/`](../docs/data-model) for the conceptual model, the schema diagram, and the in-app Power BI model view.

**Notable DAX patterns used in this report:**
- **INTERSECT pattern** — used to correctly aggregate revenue measures that span multiple fact tables without double-counting.
- **ISINSCOPE** — used to build dynamic growth-rate measures that adapt across different levels of a hierarchy (e.g., Year vs. Month).
- **RFM segmentation** — calculated via DAX measures (Recency, Frequency, Monetary) rather than pre-computed columns, so segments update live with any filter selection.
- **Cohort retention matrix** — retention percentages calculated per cohort-month using DAX time-intelligence functions.
- **ABC/Pareto supplier segmentation** — running-total and rank-based DAX measures to classify suppliers into A/B/C tiers.

---

## Data Modeling Notes

**Blank entries in date slicers.** `dim_date` is engineered to dynamically extend through December 31 of the current year, to support full-year reporting and time-intelligence functions. Because this extended range includes future dates with no matching rows in `fact_orders`, Power BI surfaced an unwanted `(Blank)` entry in date slicer visuals. This was fixed with a DAX measure, `Has_Orders`, that checks whether a given date actually has related orders — used as a filter on the date slicer as well as on some other visuals:

```dax
Has_Orders = 
IF (
    NOT ISBLANK ( COUNTROWS ( fact_orders ) ), 
    1, 
    0
)
```

---

## KPI Definitions & Fixes

- **Cash at Risk %** = failed payments ÷ (failed + paid). Measures failed *collection* only — Pending is excluded, and the ratio is of total attempted value, not of realized revenue.
- **Logistics Waste** = payments collected (Paid) on orders whose delivery failed. Despite the name, it's a refund liability, not true operational waste — no overlap with Cash at Risk, since one tracks failed payments and the other tracks paid-but-undelivered orders.
- **Mismatched Orders Count** was rebuilt: the old logic only checked `Completed` orders with an overly broad "not Delivered" rule, missing thousands of real anomalies and misclassifying thousands of normal ones. It now uses `Order_Anomaly_Flag`, based on all status combinations actually observed in the data.
- **Mismatch Rate %** denominator is **Total Orders** (not Total Completed Orders), to match the full-population scope of the new numerator.

---

## Theme

The report uses a custom-built JSON theme, designed for this project, for consistent coloring across all four pages.

---

## Related Documentation

- [Root README](../README.md) — full project walkthrough (insights, problems, recommendations)
- [SQL README](../sql/README.md) — data warehouse and analysis query documentation
- [`docs/data-model/`](../docs/data-model) — conceptual model, galaxy schema diagram, data dictionary, and Power BI model view
- [`docs/reports/`](../docs/reports) — dashboard insights, business problems, and business recommendations reports