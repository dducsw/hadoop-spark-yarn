# Practice Plan: Home Credit Risk Platform

## 1. Purpose

Build an on-premise Data Lake for financial risk intelligence using **Hadoop (HDFS), YARN, Spark, and Hive**.

- **ETL / ELT Pipelines:** Ingest and process raw transactional data across Data Lake Zones (**Raw $\rightarrow$ Cleaned $\rightarrow$ Curated**).
- **Data Modeling:** Implement Kimball Star Schema (Cleaned Zone) and Analytical Marts / One Big Table (Curated Zone).
- **Spark Optimization:** Tune partitions, broadcast joins, and shuffle memory on YARN.
- **Delivery Roadmap:** **BI & Risk Analytics first**, followed by **ML Feature Engineering & Credit Scoring**.

---

## 2. Dataset & Business Requirements (Home Credit)

**Source:** [Kaggle Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data)

### 2.1 ERD & Schema Overview

![Home Credit Data Schema](images/source_data.png)

```
application_{train|test} (SK_ID_CURR - PK)
├── bureau (SK_ID_BUREAU - PK, SK_ID_CURR - FK)
│   └── bureau_balance (SK_ID_BUREAU - FK, MONTHS_BALANCE)
└── previous_application (SK_ID_PREV - PK, SK_ID_CURR - FK)
    ├── POS_CASH_balance (SK_ID_PREV - FK, MONTHS_BALANCE)
    ├── installments_payments (SK_ID_PREV - FK, DAYS_INSTALMENT)
    └── credit_card_balance (SK_ID_PREV - FK, MONTHS_BALANCE)
```

### 2.2 Table Summary

| Table | Primary / Foreign Keys | Granularity & Content |
| :--- | :--- | :--- |
| **`application_{train\|test}`** | `SK_ID_CURR` (PK) | 1 row / applicant. Profile, demographics, external ratings (`EXT_SOURCE`), `TARGET` (0 = paid, 1 = default). |
| **`bureau`** | `SK_ID_BUREAU` (PK), `SK_ID_CURR` (FK) | 1 row / loan at other financial institutions (status, debt, overdue amounts). |
| **`bureau_balance`** | `SK_ID_BUREAU` (FK) | 1 row / month per bureau loan (`STATUS`: 0 = ok, 1-5 = overdue tiers, C = closed). |
| **`previous_application`** | `SK_ID_PREV` (PK), `SK_ID_CURR` (FK) | 1 row / previous Home Credit application (approved, refused, canceled terms). |
| **`POS_CASH_balance`** | `SK_ID_PREV`, `SK_ID_CURR` (FK) | 1 row / month per POS/cash loan (contract status, remaining terms). |
| **`installments_payments`** | `SK_ID_PREV`, `SK_ID_CURR` (FK) | 1 row / payment transaction (due vs. actual payment date and amount). |
| **`credit_card_balance`** | `SK_ID_PREV`, `SK_ID_CURR` (FK) | 1 row / month per credit card (balance, limit, drawings, overdue status). |

### 2.3 Detailed Business & Analytical Goals (Phased Roadmap)

#### Phase 1: Portfolio Risk Intelligence & Reporting (BI - Primary Focus)
The immediate business priority is to provide risk managers, credit committees, and executive stakeholders with multidimensional visibility into loan portfolio health and credit risk drivers:

1. **Portfolio Asset Quality & Delinquency Analytics:**
   - **Default & NPL Tracking:** Monitor Non-Performing Loan (NPL) rates and default ratios (`TARGET`) across active credit vintages.
   - **Portfolio at Risk (PAR):** Measure roll-rates and overdue delinquency buckets ($\text{PAR}_{30}$, $\text{PAR}_{60}$, $\text{PAR}_{90}$) to identify early warning default patterns.
   - **Payment Discipline & Underpayment Analysis:** Track repayment timeliness ($\text{Days Past Due}$) and payment shortfall ratios ($\frac{\text{Actual Paid}}{\text{Scheduled Due}}$) across monthly cycles.

2. **Customer Segmentation & Demographic Exposure:**
   - **Risk Concentration:** Analyze default distribution across demographic segments (age cohorts, income brackets, education levels, marital status, and employment tenure).
   - **Affordability & Debt Burden:** Evaluate Debt-to-Income (DTI) and Annuity-to-Income ratios across borrower profiles to assess financial stress thresholds.

3. **Product & Channel Performance:**
   - **Product Line Comparison:** Contrast risk-adjusted returns and default incidence across Cash Loans, POS Consumer Lending, and Revolving Credit Cards.
   - **Underwriting Funnel:** Track application velocity, approval rates, rejection ratios, and client cancellation rates by product type and application channel.

4. **External Bureau Exposure & Cross-Institution Debt:**
   - **Over-indebtedness Detection:** Measure total active loans and cumulative outstanding balances per customer across external financial institutions (CIC).
   - **Credit Bureau History:** Analyze historical external delinquency and credit limit utilization as cross-validation for internal underwriting.

---

#### Phase 2: Predictive Credit Scoring & Decisioning (ML - Secondary Stage)
Once the analytical reporting foundation is established, the platform extends into automated predictive intelligence:

1. **Alternative Credit Scoring Engine:**
   - Develop Probability of Default (PD) scoring models (LightGBM, XGBoost, CatBoost) specifically optimized for unbanked and thin-file applicants lacking formal banking history.
2. **Enterprise Feature Store:**
   - Engineer and maintain automated feature pipelines (200+ historical aggregations, payment velocity trends, credit utilization ratios, and external score blends).
3. **Automated Underwriting & Cutoff Calibration:**
   - Provide score outputs to feed automated underwriting policy engines, risk-based pricing tiers, and credit limit optimization.

---

### 2.4 Data Lake Multi-Zone Architecture

- **Raw Zone (`/lake/raw/`):**
  - **Objective:** Ingest and store immutable, bit-level copies of source files directly from transactional systems.
  - **Characteristics:** Schema-on-read, raw CSV format, preservation of all original columns with pipeline audit metadata (`_ingested_at`, `_source_file`).
- **Cleaned Zone (`/lake/cleaned/`):**
  - **Objective:** Standardize, cleanse, and structure conformed business entities across domains.
  - **Characteristics:** Optimized columnar format (Apache Parquet with Snappy compression), explicit type casting, deduplication, anomaly corrections (handling sentinel values and negative duration offsets), and Kimball Star Schema modeling (`Dimensions` and `Facts`).
- **Curated Zone (`/lake/curated/`):**
  - **Objective:** Serve consumer-ready, high-performance analytical datasets tailored for downstream business consumption.
  - **Characteristics:** 
    - *For Phase 1 (BI):* Dimensional data marts, pre-aggregated risk metrics, and summary tables optimized for interactive SQL queries via Hive / Spark SQL / BI dashboards.
    - *For Phase 2 (ML):* Denormalized One Big Table (OBT) feature stores with customer-level historical aggregations ready for ML model training and scoring.
