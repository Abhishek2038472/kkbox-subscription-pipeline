# 📊 SaaS Subscription Churn & Monthly Recurring Revenue (MRR) Analytics Pipeline

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.0+-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

An enterprise-scale end-to-end business intelligence and data engineering solution analyzing **825,000+ subscription accounts** representing **$108.5M in normalized Monthly Recurring Revenue (MRR)**. 

This repository documents the full analytical lifecycle: from chunked batch ingestion and relational Star Schema data warehousing in PostgreSQL to DAX-driven customer health scorecards in Power BI and executive revenue-reclamation audits.

---

## 📌 Executive Summary & Macro Financial Findings

* **Portfolio Scale:** 825,368 validated subscriber accounts across multi-year cohorts.
* **Macro Baseline Churn:** **6.52%** aggregate churn rate (53,777 canceled accounts).
* **Revenue Exposure:** **$7.15M normalized MRR lost** directly to subscriber churn (~$85.8M annualized run rate).
* **The 7.75x Renewal Friction Multiplier:** 
  * **Auto-Renew Active (`1`):** **4.0%** churn rate.
  * **Manual Renewal Active (`0`):** **31.0%** churn rate.
  * Non-auto-renewing accounts are **nearly 8x more likely to drop off**, confirming that portfolio revenue decay is primarily driven by *involuntary billing friction* rather than product dissatisfaction.
* **The High-ARPU Pricing Paradox:** Churned subscribers exhibited a higher average normalized MRR (**$133.03**) than active retained subscribers (**$131.34**), indicating severe vulnerability at full-price renewals following discounted trial expirations.
* **Payment Rail Failure Concentration:** **Carrier Direct Billing** was identified as the highest-churn payment route (~34.2%), driven by prepaid balance expirations, telco gateway limits, and lack of automated retry logic.

---

## 🏗️ System Architecture & Data Pipeline

The pipeline processes multi-gigabyte raw transactional records via chunked Python streams into an indexed relational OLAP warehouse, exposing high-performance analytical views for Power BI visualization:

┌─────────────────────────┐
│     Raw Data Layer      │  • members_v3.csv (User Profiles & Signups)
│      (KKBox Corpus)     │  • transactions_v2.csv (Subscriptions & Billings)
└────────────┬────────────┘  • train_v2.csv (Ground-Truth Churn Labels)
│
▼
┌─────────────────────────┐
│   Python Ingestion ETL  │  • Batch chunking (100K rows/chunk) to manage RAM
│ (SQLAlchemy + psycopg2) │  • Outlier imputation (User age sanitization: 10–95)
└────────────┬────────────┘  • Datetime ISO normalization & discount feature engineering
│
▼
┌─────────────────────────┐
│  PostgreSQL Warehouse   │  • Staging Layer: stg_members, stg_transactions, stg_churn_labels
│  (OLAP Star Schema)     │  • Dimensions: dim_members, dim_payment_methods
└────────────┬────────────┘  • Fact: fact_transactions (B-Tree indexed on keys/dates)
│               • Analytical View: view_saas_churn_analysis (Window partitioned)
▼
┌─────────────────────────┐
│  Power BI Analytics BI  │  • Centralized DAX Measures Table (_Measures)
│      (Import Mode)      │  • Page 1: Executive SaaS MRR & Portfolio Overview
└────────────┬────────────┘  • Page 2: Friction Diagnostics & Risk Segmentation
│
▼
┌─────────────────────────┐
│ Executive Deliverable   │  • 4-Page Board-Level Commercial Audit (python-docx)
│   (Strategy Roadmap)    │  • 3-Phase $2.85M MRR Recovery Plan
└─────────────────────────┘

## 🗄️ Relational Data Warehouse Modeling (OLAP Schema)

The analytical data mart follows an optimized **Star Schema** architecture with B-Tree indexes to ensure sub-second query performance during BI ingestion.

              ┌───────────────────────────────┐
              │          dim_members          │
              ├───────────────────────────────┤
              │ PK  member_id (VARCHAR)       │
              │     city_id (INT)             │
              │     age (INT)                 │
              │     age_cohort (VARCHAR)      │
              │     gender (VARCHAR)          │
              │     registered_via (INT)      │
              │     registration_date (DATE)  │
              └───────────────┬───────────────┘
                              │ 1
                              │
                              │ N

┌─────────────────────────────┐   │   ┌───────────────────────────────┐
│     dim_payment_methods     │   └───┤       fact_transactions       │
├─────────────────────────────┤       ├───────────────────────────────┤
│ PK  payment_method_id (INT) ├───────┤ PK  transaction_id (BIGINT)   │
│     payment_category (TEXT) │ 1   N │ FK  member_id (VARCHAR)       │
└─────────────────────────────┘       │ FK  payment_method_id (INT)   │
│     payment_plan_days (INT)   │
│     plan_tier (TEXT)          │
│     plan_list_price (INT)     │
│     actual_amount_paid (INT)  │
│     discount_amount (INT)     │
│     is_auto_renew (INT)       │
│     transaction_date (DATE)   │
│     expire_date (DATE)        │
│     normalized_mrr (NUMERIC)  │
└───────────────────────────────┘


### Core Analytical View Construction (`view_saas_churn_analysis`)
To eliminate duplicate transaction grain and identify the ground-truth churn status at the latest subscription lifecycle point, an analytical view was implemented using SQL Window Functions:

sql
CREATE OR REPLACE VIEW view_saas_churn_analysis AS
WITH ranked_transactions AS (
    SELECT 
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.member_id 
            ORDER BY f.transaction_date DESC, f.membership_expire_date DESC
        ) AS rn
    FROM fact_transactions f
)
SELECT
    m.member_id,
    m.city_id,
    m.age,
    m.age_cohort,
    m.gender,
    rt.payment_method_id,
    pm.payment_category,
    rt.payment_plan_days,
    rt.plan_tier,
    rt.actual_amount_paid,
    rt.discount_amount,
    rt.is_auto_renew,
    rt.normalized_mrr,
    rt.transaction_date AS latest_transaction_date,
    rt.membership_expire_date AS latest_expire_date,
    COALESCE(c.is_churn, 0) AS is_churn,
    CASE WHEN c.is_churn = 1 THEN 'Churned' ELSE 'Retained' END AS churn_status
FROM dim_members m
INNER JOIN ranked_transactions rt ON m.member_id = rt.member_id AND rt.rn = 1
LEFT JOIN dim_payment_methods pm ON rt.payment_method_id = pm.payment_method_id
INNER JOIN stg_churn_labels c ON m.member_id = c.msno;


🎯 Executive Revenue Reclamation RoadmapBased on the quantitative audit, a three-phase operational recovery strategy was delivered to executive stakeholders:
┌────────────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: Auto-Renew Incentive Conversion (Target: First 30 Days)               │
│ • Implement a 10% migration discount for users registering auto-renew tokens.  │
│ • Impact: Converting 25% of manual accounts preserves +$1,650,000/month.       │
├────────────────────────────────────────────────────────────────────────────────┤
│ PHASE 2: Intelligent Dunning & Pre-Notification (Target: 60 Days)              │
│ • Deploy 72-hour and 24-hour balance checks for Carrier Billing users via SMS. │
│ • Impact: Reducing carrier gateway bounce rates preserves +$480,000/month.     │
├────────────────────────────────────────────────────────────────────────────────┤
│ PHASE 3: Long-Term Plan Packaging & Commitment (Target: 90 Days)               │
│ • Package discounted 90-day and 365-day tiers to transition users off 30-days. │
│ • Impact: Migrating 15% of accounts to extended plans protects +$720,000/month.│
└────────────────────────────────────────────────────────────────────────────────┘

Repository File Structure:

kkbox-subscription-pipeline/
│
├── data/
│   ├── raw/                              # Source transactional CSVs (git-ignored)
│   └── processed/                        # Analytical extracts and validation sets
│
├── scripts/
│   ├── etl_pipeline.py                   # Automated chunked ingestion & ETL script
│   ├── generate_saas_executive_report.py # Automated 4-page Word audit generator
│   └── generate_resume_docx.py           # 1-page ATS-optimized resume generator
│
├── sql/
│   ├── 01_staging_schema.sql             # Staging table definitions
│   ├── 02_star_schema_and_churn.sql      # Fact/Dimension DDL, Indexes & Views
│   └── 03_cohort_retention_queries.sql   # Advanced window-function queries
│
├── bi/
│   └── kkbox_saas_churn.pbix             # Production 2-page Power BI dashboard
│
├── docs/
│   ├── Executive_SaaS_Churn_Audit_Report.docx # Formatted 4-page executive brief
│   └── Executive_SaaS_Churn_Audit_Report.pdf  # Final board deliverable
│
├── .gitignore                            # Ignores large raw CSV files & venv
├── requirements.txt                      # Python dependencies (pandas, docx, sqlalchemy)
└── README.md                             # Project documentation

 Project documentation
