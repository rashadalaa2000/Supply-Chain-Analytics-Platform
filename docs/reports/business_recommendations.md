# CanRoute — Canadian B2B Last-Mile Delivery Analytics
## Business Recommendations Report (2020–2025)

> Scope: Recommendations addressing the business problems previously identified from the CanRoute dashboard (Executive Overview, Logistics & Delivery Operations, Retailer Performance, Financial Health). Each recommendation is linked to the problem it addresses.

---

## 1. Revenue Leakage & Financial Integrity

**Problem:** ~31% of GMV never converts to realized revenue; Collection Rate is only 69.31%; Mismatch Rate is 16.55%; Anomaly Cash Exposure ($18.81M) exceeds Outstanding Receivables ($8.66M); $11.25M is lost in the cash reconciliation waterfall; 21K invoices sit in "Not Paid" status.

- Introduce automated order/delivery/payment status validation at the point of status change, so mismatches are flagged and corrected before they reach the reconciliation stage, rather than being discovered after the fact.
- Prioritize root-cause analysis on the **Anomaly Cash Exposure** category specifically, since it is more than double Outstanding Receivables — treat it as a separate, higher-priority workstream from standard AR collections.
- Set a formal SLA for orders sitting in "Not Paid" status (e.g., escalate to collections or write-off review after a fixed number of days), since this bucket currently outweighs the 31–60 and 60+ day aging buckets combined.
- Establish a monthly reconciliation checkpoint that tracks the GMV → Cash Collected → Realized Revenue waterfall as a single KPI chain, making leakage at each stage visible to finance and operations together rather than in isolation.
- Investigate the drivers of "Wasted Delivery Effort" ($2.49M) to determine whether it stems from failed-delivery attempts, address/data errors, or scheduling — this cost is currently being incurred without a clear attribution.

---

## 2. Delivery & Operational Performance

**Problem:** 63.62% of orders are delayed; Severe Delay (21.3K orders) is the second-largest delay bucket; on-time performance collapses to ~20% every Jan–Mar; 6.4K Critical-severity anomalies exist; ~10% of orders sit in an ambiguous, unclassified delivery state; provincial cash-at-risk varies by up to ~2 points.

- Conduct a focused review of the **Jan–Mar on-time collapse**, since it repeats every year and represents a sustained multi-month underperformance window rather than a one-off event — align staffing, carrier capacity, or route planning to this seasonal pattern ahead of each Q1.
- Treat **Severe Delay** orders as a distinct operational tier (not a generic "late" bucket) given their volume (21.3K) is close to the Early bucket — build a dedicated tracking/escalation path for orders that cross into the Severe threshold.
- Resolve the ambiguous ~10% of orders that are neither "Delivered" nor formally "Failed/Cancelled" by tightening status definitions and closing the gap in the operational status taxonomy, so operational KPIs reflect the true delivery outcome for every order.
- Direct the Critical-severity anomaly volume (6.4K) to a fast-track review queue, since it exceeds Medium-severity volume and represents the highest-risk order population.
- Benchmark the highest cash-at-risk provinces (MB, ON, QC) against the lowest (NS, AB, SK) to identify what operational or geographic factors explain the ~2-point spread, and apply learnings from the better-performing provinces where feasible.
- Since headline on-time rates are similar in winter vs. non-winter months but a secondary delivery outcome diverges sharply (40.99% vs 19.51%), track that specific outcome metric separately during winter months rather than relying on the on-time rate alone to judge winter performance.

---

## 3. Retailer & Revenue Concentration Risk

**Problem:** Top 20% of retailers generate 76.27% of revenue; Champions + Loyal + Potential Loyalists = 92.5% of revenue; 47 Core suppliers generate 69.47% of supplier revenue; Central Canada = 68% of revenue vs. Atlantic Canada = 2%; 2022 had negative retailer growth; the 2019 cohort shows retention decay from 86.46% to 72.92%; 4K retailers are At Risk and 1K are Hibernating.

- Develop a formal retention program targeted specifically at the **At Risk (4K)** and **Hibernating (1K)** RFM segments before they convert to Lost, since these segments represent early-stage attrition that is still recoverable.
- Given the extreme reliance on the top 20% of retailers (76.27% of revenue), build a structured account-management or key-account program for this group to protect the revenue base, alongside a parallel effort to grow mid-tier (Potential Loyalists) retailers into Loyal/Champion status.
- Diversify supplier dependency by developing a subset of the "Mid" (B) supplier segment toward "Core" (A) status, reducing reliance on the current 47-supplier concentration.
- Evaluate targeted growth initiatives in **Atlantic Canada and Western Canada**, given their combined 32% share versus Central Canada's 68% — geographic expansion here would reduce single-region dependency.
- Investigate the causes of the 2022 negative retailer growth (−1.83%) specifically, to determine whether it was a one-time event (market, onboarding, or churn-driven) and to prevent recurrence, given growth has since recovered and accelerated through 2025.
- Apply the retention pattern observed in the 2019 cohort (decay from 86% to 73% within 6 months) as an early-warning benchmark for newer cohorts, enabling proactive outreach before newer retailers reach a similar decay point.

---

## 4. Cross-Cutting Structural Recommendations

**Problem:** Leakage compounds sequentially from GMV to realized revenue; operational status issues and financial mismatches stem from the same order population; concentration risk exists simultaneously across geography, retailers, RFM segments, and suppliers.

- Establish a **single cross-functional KPI dashboard view** (already partially achieved via this Power BI build) that tracks the GMV-to-revenue waterfall, delay rates, and concentration metrics together, since these problems are structurally linked rather than independent.
- Since the Mismatch Rate closely tracks the share of orders in Delivery Exception/Cancelled/In Progress status, route financial reconciliation review and operational status review through the same team or process, rather than treating them as separate finance vs. operations workstreams.
- Given that concentration risk appears across every structural dimension (geography, retailers, RFM segments, suppliers) simultaneously, treat diversification as a portfolio-level objective — set explicit concentration thresholds (e.g., max % revenue from top region/segment/supplier group) and monitor them alongside growth KPIs, rather than addressing each dimension in isolation.
