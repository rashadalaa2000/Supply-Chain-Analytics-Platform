# CanRoute — Canadian B2B Last-Mile Delivery Analytics
## Dashboard Insight Report (2020–2025)

> Scope: Insights extracted from the four analytical pages of the CanRoute Power BI dashboard — Executive Overview, Logistics & Delivery Operations, Retailer Performance, and Financial Health. This document contains **observations derived from the data only** — no recommendations or problem statements.

---

## 1. Executive Overview

- Realized Revenue stands at **$77M** against a **Total GMV of $111.66M** (Financial Health page), meaning roughly **31% of gross order value never converts to realized revenue**.
- The **Mismatch Rate is 16.55%**, while the **Collection Rate is 69.31%** — these two figures move together, indicating that a large share of the revenue gap is tied to order/payment/delivery status inconsistencies rather than pure non-payment.
- Realized Revenue grew consistently year over year: **$8.6M (2020) → $10.6M (2021) → $12.9M (2022) → $13.6M (2023) → $15.5M (2024) → $16.0M (2025)**. The growth rate visibly decelerates after 2022 — the 2020→2022 jump (~+50%) is much steeper than the 2023→2025 jump (~+18%).
- Revenue is heavily geographically concentrated: **Central Canada = $52M (68%)**, **Western Canada = $23M (30%)**, **Atlantic Canada = only $2M (2%)** of the $77M realized revenue.
- Product category revenue share (based on the $90M pre-mismatch total shown on this page) is led by **Food & Beverage ($41M, 46%)**, followed by **Industrial & Packaging ($30M, 33%)** and **Household & Cleaning ($19M, 21%)**.
- Payment behavior is split between **Electronic Payment (64.6%)** and **Credit Terms (31.6%)**, with **Cash at only 3.8%** — electronic and credit together account for over 96% of realized revenue.
- Supplier revenue concentration follows a classic Pareto pattern: **47 "Core" (A) suppliers generate 69.47%** of segment revenue, **26 "Mid" (B) suppliers generate 24.76%**, and **7 "Long-tail" (C) suppliers generate only 5.77%** — the 47 A-suppliers alone outproduce the other 33 suppliers combined by roughly 2.8x.

---

## 2. Logistics & Delivery Operations

- The **Delay Rate is 63.62%**, meaning nearly two-thirds of all orders experience some form of delay relative to their expected delivery window.
- Order Distribution by Delay Status: **Early = 26.5K**, **Severe Delay (12–72h) = 21.3K**, **Moderate Delay (2–6h) = 12.3K**, **Minor Delay (0–2h) = 8.9K**, **Major Delay (6–12h) = 5.2K**, **Pending = 4.0K**, **Extreme Delay (72h+) = 2.5K**. Notably, **Severe Delay is the second-largest bucket overall**, larger than Minor and Moderate Delay combined being close but not exceeding it — the delay distribution is bimodal (large "on time/early" cluster and a large "severe delay" cluster), rather than a smooth gradient.
- **On-Time Rate % by month** shows a strong seasonal pattern: on-time performance sits near **20% in Jan–Mar**, rises sharply to a stable **~40% plateau from Apr through Nov**, then drops again toward year-end (Dec). The transition from low to high performance is abrupt (a step-change around March–April) rather than gradual.
- **Cash at Risk % by province** ranges narrowly from **2.35% (Nova Scotia) to 4.28% (Manitoba)**, with Central provinces (ON, QC) and MB sitting above 3.7% while Atlantic/Western provinces (NS, AB, SK) sit below 3.2% — a roughly 1.9-point spread across all provinces.
- **Weather Impact on Delivery Timeliness**: for the "on-time" category, cold and non-cold cities perform almost identically (**47.54% vs 46.97%**, non-winter vs winter month). However, for the second category shown, the gap widens substantially (**40.99% vs 19.51%**) — indicating winter-month effects are concentrated in a specific delivery outcome rather than affecting on-time performance broadly.
- **Cross-Status Anomalies by Severity**: **Critical = 6.4K**, **High = 4.1K**, **Medium = 3.0K** orders. Critical-severity anomalies alone (6.4K) exceed High and Medium combined severity difference, making Critical the dominant anomaly tier by volume.
- Of 82K total orders, only **67K (≈82%) show as "Orders Delivered"** on this page, while Delivery Failure Rate (2.91%) and Cancellation Rate (4.88%) together account for under 8% — implying a further ~10% of orders sit in delay/pending states not yet classified as delivered or failed.

---

## 3. Retailer Performance

- Of **489 Active Retailers**, the **Average Order Value (AOV) is $1.36K** and **Order Frequency is 116.86**, producing an **Average Revenue Per Retailer (ARPR) of $159K**.
- **Revenue Concentration (Top 20%) = 76.27%** — the top fifth of retailers generate more than three-quarters of total revenue, a highly concentrated distribution.
- **Active Retailer Growth (YoY)** was volatile early on: **+3.23% (2021) → −1.83% (2022) → +0.80% (2023) → +3.69% (2024) → +6.87% (2025)**. 2022 is the only year with negative growth, and the growth rate has accelerated in the two most recent years, with 2025 posting the highest YoY growth of the period.
- **RFM Segmentation** shows retailers concentrated at the healthy end: **Champions = 29K**, **Loyal Customers = 28K**, **Potential Loyalists = 15K** — these three segments alone account for the large majority of the base, while **At Risk (4K)**, **New Customers (3K)**, **Hibernating (1K)**, and **Lost Customers (near 0)** are comparatively small.
- The **Pareto Analysis by Segment** confirms the RFM pattern: **Champions alone contribute 46.65%** of realized revenue, and cumulative revenue reaches **78.65% by Loyal Customers** and **92.50% by Potential Loyalists** — meaning the top three RFM segments account for over 92% of total revenue, while the remaining four segments (At Risk, New, Hibernating, Lost) contribute under 8% combined.
- The **Annual Cohort Retention Matrix** shows the 2019 cohort declining steadily from **86.46% (month 1) to 72.92% (month 6)**, a clear erosion pattern over its first six months. The 2020 cohort behaves differently, holding in the **86–100%** range across all six months with less decay. Cohorts from **2023–2025 show 100% retention**, though this is expected given their limited observation window (fewer elapsed months since acquisition) rather than necessarily reflecting stronger retention behavior.

---

## 4. Financial Health

- **Cash Collected = $83.74M** against **Outstanding Receivables of $8.66M** and **Anomaly Cash Exposure of $18.81M** — the anomaly exposure figure is more than double the outstanding receivables, making status-mismatch-driven exposure a larger dollar figure than standard unpaid receivables.
- **Cash at Risk % = 3.61%**, closely aligned with the province-level range seen on the Logistics page (2.35%–4.28%), suggesting the national blended rate falls roughly mid-range across provinces.
- The **Cash Reconciliation & Revenue Leakage Waterfall** starts at **Cash Collected ($83.74M)**, adds **Cash Pending (+$16.68M)**, then subtracts **Cash at Risk (−$2.90M)**, **Failed Delivery Exposure (−$3.00M)**, and **Cash Lost/Reversed (−$5.35M)**, arriving at a **Total of $89.17M**. The three deduction categories (Cash at Risk, Failed Delivery Exposure, Cash Lost/Reversed) sum to **$11.25M** in combined leakage against the $100.42M gross (Collected + Pending).
- **GMV & Order Distribution by Operational Status**: **Fulfilled = $91.15M / 66,628 orders**, **Delivery Exception = $14.13M / 10,438 orders**, **Cancelled = $5.35M / 3,995 orders**, **In Progress = $1.03M / 776 orders**, for a **Total of $111.66M / 81,837 orders**. Fulfilled orders represent **~81.5% of total order count** but **~81.6% of total GMV**, indicating GMV per order is roughly consistent across the Fulfilled segment relative to the overall average — unlike Delivery Exception orders, which represent 12.8% of orders but only 12.7% of GMV, a near-proportional relationship as well.
- **Monthly Cash Inflow vs. Pending Closure Trend** rises gradually from January through October, then shows a sharp acceleration in **November–December**, mirroring the steep uptick seen in the Executive Overview's revenue growth trend for the most recent period.
- **AR Aging distribution**: **0–30 days = 47K** (the dominant bucket), **31–60 days = 9K**, **60+ days = 5K**, and **Not Paid = 21K**. The "Not Paid" bucket (21K) is larger than the 31–60 and 60+ day buckets combined (14K), making unpaid (non-aging) receivables a bigger share of the outstanding base than aged-but-paid receivables.

---

## 5. Cross-Dashboard Observations

- The **$111.66M GMV → $89.17M reconciled cash → $83.74M actually collected → $77M realized revenue** figures form a consistent waterfall across the four pages, each layer stripping out a further category of leakage (mismatches, pending amounts, risk/failure/reversal exposure).
- The **16.55% Mismatch Rate** (Executive Overview) is numerically close to the **share of orders in Delivery Exception + Cancelled + In Progress status** (14,438 + 776 = ~15,209 of 81,837 ≈ 18.6%), suggesting these non-Fulfilled operational statuses are a primary driver of the mismatch figure.
- Both the Logistics page (Delay Rate 63.62%) and Financial Health page (Cash at Risk 3.61%) point to a pattern where **operational delay is common but does not proportionally translate into financial risk** — the delay rate is roughly 17x higher than the cash-at-risk rate, indicating most delays are absorbed without direct cash exposure.
- Revenue concentration appears at multiple levels simultaneously: **geographic (Central Canada 68% of revenue)**, **retailer (top 20% = 76.27% of revenue)**, **RFM segment (top 3 segments = 92.5% of revenue)**, and **supplier (Core suppliers = 69.47% of revenue)** — concentration is a recurring structural pattern across every dimension of the business, not isolated to one area.
