# SaaS Subscription Churn & MRR Analytics Pipeline

An end-to-end data pipeline and business intelligence suite analyzing **825,000+ subscription accounts** representing **$108.5M in Monthly Recurring Revenue (MRR)**. Built with Python, PostgreSQL, and Power BI.

---

## 📌 Key Business Findings

* **Macro Churn Baseline:** **6.52%** aggregate churn rate (53,777 accounts) totaling **$7.15M lost MRR/month**.
* **The 7.75x Auto-Renew Penalty:** 
  * **Auto-Renew Active (`1`):** **4.0%** churn rate.
  * **Manual Renewal (`0`):** **31.0%** churn rate.
  * Involuntary billing friction—not product dissatisfaction—is the single primary driver of customer attrition.
* **Pricing Paradox:** Churned subscribers had a higher average MRR (**$133.03**) than retained subscribers (**$131.34**), exposing full-price drop-offs after initial promos.
* **Channel Vulnerability:** **Carrier Direct Billing** had the highest churn rate (~34%) due to prepaid balance limits and telco gateway drop-offs.

---

## 🏗️ Architecture & Tech Stack

* **Extraction & Ingestion:** Python (`pandas`, `SQLAlchemy`, `psycopg2`) chunking multi-GB CSVs in 100K batches.
* **Data Warehouse:** PostgreSQL Star Schema (`dim_members`, `dim_payment_methods`, `fact_transactions`) with B-Tree indexes and a consolidated analytical view (`view_saas_churn_analysis`).
* **BI & Analytics:** Power BI Desktop with dedicated DAX metrics table (`Total MRR`, `Churn Rate`, `ARPU`, `Auto vs Manual Churn`).
* **Deliverable:** 4-page executive commercial audit report generated via `python-docx`.

---

## 🗄️ Core Star Schema & SQL View

```sql
CREATE OR REPLACE VIEW view_saas_churn_analysis AS
WITH ranked_transactions AS (
    SELECT f.*, ROW_NUMBER() OVER (
        PARTITION BY f.member_id ORDER BY f.transaction_date DESC, f.membership_expire_date DESC
    ) AS rn
    FROM fact_transactions f
)
SELECT
    m.member_id, m.age_cohort, m.gender, pm.payment_category,
    rt.plan_tier, rt.actual_amount_paid, rt.is_auto_renew, rt.normalized_mrr,
    COALESCE(c.is_churn, 0) AS is_churn,
    CASE WHEN c.is_churn = 1 THEN 'Churned' ELSE 'Retained' END AS churn_status
FROM dim_members m
INNER JOIN ranked_transactions rt ON m.member_id = rt.member_id AND rt.rn = 1
LEFT JOIN dim_payment_methods pm ON rt.payment_method_id = pm.payment_method_id
INNER JOIN stg_churn_labels c ON m.member_id = c.msno;
