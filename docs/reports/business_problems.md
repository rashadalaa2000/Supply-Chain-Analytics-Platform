# CanRoute — Canadian B2B Last-Mile Delivery Analytics
## Business Problems Report (2020–2025)

> Scope: Business problems identified from the CanRoute dashboard (Executive Overview, Logistics & Delivery Operations, Retailer Performance, Financial Health). This document states **problems only** — no recommendations or solutions.

---

## 1. Revenue Leakage & Financial Integrity

- **Gross-to-realized revenue gap**: Of **$111.66M** in total GMV, only **$77M** is realized revenue — nearly **one-third of order value ($34.66M) never converts to recognized revenue**.
- **Low Collection Rate**: Only **69.31%** of expected value is actually collected, meaning close to a third of billed/expected cash is not collected on schedule.
- **High Mismatch Rate**: **16.55%** of orders carry status inconsistencies across order/delivery/payment states, directly reducing revenue reliability.
- **Anomaly Cash Exposure exceeds standard receivables**: Anomaly Cash Exposure is **$18.81M**, more than **double** the Outstanding Receivables figure of **$8.66M** — cash tied up in status mismatches is a larger problem than normal overdue collections.
- **Cash reconciliation leakage**: Across the waterfall, **$11.25M** is lost between Cash Pending and final reconciled cash (Cash at Risk + Failed Delivery Exposure + Cash Lost/Reversed combined), before revenue is even realized.
- **Large "Not Paid" AR bucket**: **21K** invoices sit in "Not Paid" status — larger than the 31–60 day and 60+ day aging buckets combined (14K) — indicating a significant volume of receivables never entering a normal aging/collection cycle.
- **Wasted Delivery Effort**: **$2.49M** in delivery effort is classified as wasted, representing cost incurred without corresponding realized value.

---

## 2. Delivery & Operational Performance

- **Majority of orders are delayed**: The overall **Delay Rate is 63.62%** — the majority of all orders do not arrive within their expected delivery window.
- **Severe Delay is a major volume bucket, not an edge case**: **21.3K orders** fall into "Severe Delay (12–72h)" — the second-largest delay bucket after "Early," and larger than "Minor" and "Major" delay combined in the surrounding range. Delay severity is not gradually distributed; a large share of delayed orders lands in the most severe categories.
- **Seasonal collapse in on-time performance**: On-Time Rate % drops to roughly **20% in January–March** each year, versus a stable **~40% plateau from April to November** — a sustained multi-month period where on-time performance is roughly half the rest-of-year baseline.
- **High-severity anomalies dominate**: **6.4K "Critical"**-severity order anomalies exist — more volume than the "Medium" severity tier (3.0K), meaning anomaly volume skews toward the most serious classification rather than the least.
- **Unclassified order volume**: Of 82K total orders, only 67K are marked "Delivered," and Delivery Failure (2.91%) + Cancellation (4.88%) account for under 8% combined — leaving roughly **10% of orders in an ambiguous state** (delayed/pending, neither delivered nor formally failed/cancelled).
- **Provincial cash-at-risk spread**: Cash at Risk % varies from **2.35% to 4.28%** across provinces — a gap of nearly 2 percentage points between the best- and worst-performing provinces on this metric.
- **Winter-driven divergence in specific delivery outcomes**: While overall on-time rates are similar between winter and non-winter months (47.54% vs 46.97%), a secondary delivery-outcome metric shows a much larger gap (40.99% vs 19.51%) between cold and non-cold cities — indicating winter conditions materially affect certain outcomes even where the headline on-time metric appears stable.

---

## 3. Retailer & Revenue Concentration Risk

- **Extreme revenue concentration in the retailer base**: The **top 20% of retailers generate 76.27%** of total revenue — the business is heavily dependent on a small subset of its 489 active retailers.
- **RFM segment concentration**: **Champions alone contribute 46.65%** of realized revenue, and the top three RFM segments (Champions, Loyal, Potential Loyalists) account for **92.50%** of revenue combined — meaning At Risk, New, Hibernating, and Lost customers together contribute under 8%, leaving little revenue diversification outside the top tiers.
- **Supplier concentration**: Just **47 "Core" suppliers generate 69.47%** of supplier-segment revenue, while **7 "Long-tail" suppliers generate only 5.77%** — heavy reliance on a small supplier group.
- **Geographic concentration**: **Central Canada accounts for 68% ($52M) of realized revenue**, while **Atlantic Canada contributes only 2% ($2M)** — revenue is not geographically diversified.
- **Negative retailer growth year**: Active Retailer Growth (YoY) was **negative in 2022 (−1.83%)**, the only contraction year in the 2021–2025 series, following a positive 2021.
- **Cohort retention decay**: The 2019 retailer cohort's retention rate declined from **86.46% in month 1 to 72.92% by month 6** — a clear erosion pattern within the first six months of that cohort's lifecycle.
- **At Risk and Hibernating segments exist at meaningful scale**: **4K retailers are "At Risk"** and **1K are "Hibernating"**, representing retailer relationships trending toward attrition.

---

## 4. Cross-Cutting Structural Problems

- **Compounding leakage across the value chain**: The same order base loses value at multiple sequential stages — from GMV ($111.66M) → reconciled cash ($89.17M) → cash collected ($83.74M) → realized revenue ($77M) — indicating leakage is not isolated to one process but occurs cumulatively across operations, cash handling, and revenue recognition.
- **Overlap between operational and financial problems**: The **16.55% Mismatch Rate** aligns closely with the share of orders in Delivery Exception, Cancelled, and In Progress statuses (~18.6% of orders) — operational status problems and financial reconciliation problems are linked to the same underlying order population.
- **Concentration risk is systemic, not isolated**: High concentration appears simultaneously across **geography (68% in one region)**, **retailers (top 20% = 76% of revenue)**, **RFM segments (top 3 = 92.5% of revenue)**, and **suppliers (Core group = 69.47% of revenue)** — the business carries concentration risk across nearly every structural dimension at once, rather than in a single isolated area.
