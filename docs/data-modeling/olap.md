# Home Credit Default Risk: OLAP Dimensional Modeling Architecture

## 1. Overview & Dimensional Architecture Philosophy

In modern Fintech and consumer lending data platforms, the **Online Analytical Processing (OLAP)** layer must serve two critical analytical requirements with sub-second latency:
1. **Executive & Risk Portfolio Analytics**: Vintage analysis, delinquency roll rates, non-performing loan (NPL) ratios, underwriting approval funnels, and merchant partner exposure.
2. **Feature Marts for Predictive Modeling**: Consistent point-in-time behavioral aggregations, external credit bureau profiles, and delinquency velocity metrics for credit scoring engines.

Following the **Kimball Dimensional Modeling methodology**, this architecture transitions data across the three Medallion Lakehouse tiers into a high-performance **Constellation (Galaxy) Schema**:

- **Raw Layer (`hive.raw_credit_risk.*`)**: Ingests raw source CSV files into immutable Parquet tables on HDFS (`/raw/credit_risk/*`) with strict contract-first schemas and ingestion metadata (`_ingested_at`).
- **Stage Layer (`hive.stage_credit_risk.*`)**: Cleans, deduplicates, and casts data into standardized types (`Decimal(18,2)`, timestamps, integer keys) stored on HDFS (`/stage/credit_risk/*`).
- **Curated Layer (`hive.credit_risk.*`)**: Builds conformed dimensions (`hive.credit_risk.dim_*`) and fact tables (`hive.credit_risk.fact_*`) stored on HDFS (`/curated/credit_risk/*`) in Parquet/Snappy format.
- **OLAP Serving Layer (`ClickHouse`)**: Exports curated facts and materialized aggregations into ClickHouse `MergeTree` and `AggregatingMergeTree` tables for sub-second BI query execution.

---

## 2. Enterprise Data Warehouse (EDW) Bus Matrix

The **Data Warehouse Bus Matrix** establishes conformed dimensions shared across all lending business processes, eliminating data silos and enabling cross-functional drill-downs across the `hive.credit_risk` layer.

| Business Process / Curated Fact Table | Grain | Fact Type | `dim_customer` | `dim_loan_product` | `dim_merchant_channel` | `dim_bureau_source` | `dim_delinquency_bucket` | `dim_application_decision` | `dim_relative_time` |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`hive.credit_risk.fact_loan_application`** | 1 row per credit application | Transaction / Snapshot | **X** | **X** | **X** | | | **X** | **X** |
| **`hive.credit_risk.fact_installment_payment`** | 1 row per installment transaction | Transaction | **X** | **X** | | | **X** | | **X** |
| **`hive.credit_risk.fact_monthly_loan_snapshot`** | 1 row per active loan per month | Periodic Snapshot | **X** | **X** | **X** | | **X** | | **X** |
| **`hive.credit_risk.fact_bureau_credit`** | 1 row per external credit line | Transaction | **X** | **X** | | **X** | | | **X** |
| **`hive.credit_risk.fact_monthly_bureau_snapshot`** | 1 row per external line per month | Periodic Snapshot | **X** | | | **X** | **X** | | **X** |

---

## 3. Dimensional Relationship & Cardinality Matrix

### Dimension-to-Fact Relationship Mapping

| Parent Dimension (`hive.credit_risk`) | Child Fact Table (`hive.credit_risk`) | Join Foreign Key | Cardinality | Business Semantics |
| :--- | :--- | :--- | :---: | :--- |
| `dim_customer` | `fact_loan_application` | `sk_customer_key` | $1 : N$ | Customer demographic & risk profile at time of loan origination. |
| `dim_loan_product` | `fact_loan_application` | `sk_product_key` | $1 : N$ | Product classification (Cash loan, Revolving loan, POS loan). |
| `dim_merchant_channel` | `fact_loan_application` | `sk_channel_key` | $1 : N$ | Retail partner POS channel, seller industry, and goods category. |
| `dim_application_decision` | `fact_loan_application` | `sk_decision_key` | $1 : N$ | Underwriting decision outcome (Approved, Refused, Canceled). |
| `dim_relative_time` | `fact_loan_application` | `sk_time_key` | $1 : N$ | Temporal relative offset and application vintage cohort. |
| `dim_customer` | `fact_installment_payment` | `sk_customer_key` | $1 : N$ | Customer making the scheduled or actual loan repayment. |
| `dim_loan_product` | `fact_installment_payment` | `sk_product_key` | $1 : N$ | Product type associated with the installment repayment. |
| `dim_delinquency_bucket` | `fact_installment_payment` | `sk_dpd_bucket_key` | $1 : N$ | Delinquency severity bucket for delayed or missed payments. |
| `dim_relative_time` | `fact_installment_payment` | `sk_time_key` | $1 : N$ | Relative payment due date and entry date offset. |
| `dim_customer` | `fact_monthly_loan_snapshot` | `sk_customer_key` | $1 : N$ | Customer holding active credit contract during balance snapshot. |
| `dim_loan_product` | `fact_monthly_loan_snapshot` | `sk_product_key` | $1 : N$ | Loan product type (POS Cash loan vs Revolving Credit Card). |
| `dim_delinquency_bucket` | `fact_monthly_loan_snapshot` | `sk_dpd_bucket_key` | $1 : N$ | Monthly DPD classification (Current, 30 DPD, 60 DPD, 90+ DPD). |
| `dim_relative_time` | `fact_monthly_loan_snapshot` | `sk_time_key` | $1 : N$ | Relative historical month (`MONTHS_BALANCE`). |
| `dim_customer` | `fact_bureau_credit` | `sk_customer_key` | $1 : N$ | External debt positions held by the customer across lenders. |
| `dim_bureau_source` | `fact_bureau_credit` | `sk_bureau_source_key` | $1 : N$ | External credit type (Credit card, Consumer loan, Mortgage). |
| `fact_bureau_credit` | `fact_monthly_bureau_snapshot` | `sk_bureau_credit_key` | $1 : N$ | Longitudinal monthly status of external credit bureau accounts. |
| `dim_delinquency_bucket` | `fact_monthly_bureau_snapshot` | `sk_dpd_bucket_key` | $1 : N$ | External bureau status mapping (Closed, 0 DPD, 1-30 DPD, 120+ DPD). |
| `dim_relative_time` | `fact_monthly_bureau_snapshot` | `sk_time_key` | $1 : N$ | Monthly historical snapshot window relative to application date. |

---

## 4. Curated Dimension Tables Reference & Schema

### 1. `hive.credit_risk.dim_customer`
- **Granularity**: 1 row per customer (or per customer profile version under SCD Type 2).
- **HDFS Path**: `/curated/credit_risk/dim_customer`
- **Primary Key**: `sk_customer_key` (`BigInt` surrogate key)
- **Business Key**: `sk_id_curr` (`Int` from `hive.stage_credit_risk.stage_application_train` / `stage_application_test`)
- **SCD Policy**: **Type 2** for income, family status, and housing changes; **Type 1** for static demographics.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_customer_key` | `BIGINT` | PK | Surrogate primary key generated via hash/monotonically increasing ID. |
| `sk_id_curr` | `INT` | BK | Natural business identifier of the customer. |
| `code_gender` | `STRING` | Attribute | Customer gender (`M`, `F`, `XNA`). |
| `flag_own_car` | `STRING` | Attribute | Flag indicating whether the customer owns a car (`Y`, `N`). |
| `flag_own_realty` | `STRING` | Attribute | Flag indicating whether the customer owns real estate (`Y`, `N`). |
| `cnt_children` | `INT` | Attribute | Number of children reported by the customer. |
| `cnt_fam_members` | `FLOAT` | Attribute | Total count of family members. |
| `amt_income_total` | `DECIMAL(18,2)` | Attribute | Total annual/monthly income of the customer. |
| `name_income_type` | `STRING` | Attribute | Income source category (`Working`, `Commercial associate`, `Pensioner`, `State servant`). |
| `name_education_type` | `STRING` | Attribute | Highest level of education attained. |
| `name_family_status` | `STRING` | Attribute | Marital and family status (`Married`, `Single / not married`, `Civil marriage`, `Widow`). |
| `name_housing_type` | `STRING` | Attribute | Housing situation (`House / apartment`, `With parents`, `Rented apartment`, `Municipal housing`). |
| `occupation_type` | `STRING` | Attribute | Customer job role (`Laborers`, `Sales staff`, `Core staff`, `Managers`, `Drivers`). |
| `organization_type` | `STRING` | Attribute | Industry of employer (`Business Entity Type 3`, `Self-employed`, `Other`, `Medicine`). |
| `age_years` | `INT` | Derived | Computed customer age: $\text{floor}(\text{DAYS\_BIRTH} / -365.25)$. |
| `employed_years` | `INT` | Derived | Computed employment length: $\text{floor}(\text{DAYS\_EMPLOYED} / -365.25)$. |
| `valid_from` | `TIMESTAMP` | SCD Meta | Effective start timestamp of customer record. |
| `valid_to` | `TIMESTAMP` | SCD Meta | Effective expiration timestamp (`9999-12-31 23:59:59` for active). |
| `is_current` | `BOOLEAN` | SCD Meta | Flag indicating latest active record version (`true`/`false`). |

---

### 2. `hive.credit_risk.dim_loan_product`
- **Granularity**: 1 row per unique loan product offering.
- **HDFS Path**: `/curated/credit_risk/dim_loan_product`
- **Primary Key**: `sk_product_key` (`Int` surrogate key)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_product_key` | `INT` | PK | Surrogate primary key. |
| `name_contract_type` | `STRING` | Attribute | Raw contract type (`Cash loans`, `Revolving loans`, `Consumer loans`). |
| `portfolio_category` | `STRING` | Attribute | Portfolio line (`Secured`, `Unsecured Term`, `Revolving Credit`). |
| `product_group` | `STRING` | Attribute | Aggregated product group (`Personal Cash`, `Merchant POS Line`, `Credit Card`). |
| `is_revolving` | `BOOLEAN` | Attribute | Indicator whether the facility is revolving credit. |

---

### 3. `hive.credit_risk.dim_merchant_channel`
- **Granularity**: 1 row per unique distribution channel, seller industry, and goods category.
- **HDFS Path**: `/curated/credit_risk/dim_merchant_channel`
- **Primary Key**: `sk_channel_key` (`Int` surrogate key)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_channel_key` | `INT` | PK | Surrogate primary key. |
| `channel_type` | `STRING` | Attribute | Acquisition channel (`Country-wide`, `Contact center`, `Credit and cash offices`, `Stone`). |
| `name_type_suite` | `STRING` | Attribute | Customer accompaniment suite (`Unaccompanied`, `Family`, `Spouse/partner`). |
| `name_goods_category` | `STRING` | Attribute | Financed asset category (`Mobile`, `Consumer Electronics`, `Computers`, `Furniture`, `Auto`). |
| `name_seller_industry` | `STRING` | Attribute | Retail partner industry (`Consumer electronics`, `Connectivity`, `Furniture`, `Industry`). |
| `name_yield_group` | `STRING` | Attribute | Risk yield pricing tier (`low_normal`, `middle`, `high`, `low_action`). |

---

### 4. `hive.credit_risk.dim_delinquency_bucket`
- **Granularity**: 1 row per standard delinquency aging bucket.
- **HDFS Path**: `/curated/credit_risk/dim_delinquency_bucket`
- **Primary Key**: `sk_dpd_bucket_key` (`Int` surrogate key)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_dpd_bucket_key` | `INT` | PK | Surrogate primary key. |
| `bucket_code` | `STRING` | Attribute | Standard bracket code (`B0`, `B1`, `B2`, `B3`, `B4`, `B5`, `NPL`). |
| `bucket_name` | `STRING` | Attribute | Human-readable bracket (`Current / 0 DPD`, `1-30 DPD`, `31-60 DPD`, `61-90 DPD`, `90+ DPD / NPL`). |
| `dpd_min` | `INT` | Attribute | Minimum Days Past Due bound (inclusive). |
| `dpd_max` | `INT` | Attribute | Maximum Days Past Due bound (inclusive, `99999` for top bracket). |
| `is_npl` | `BOOLEAN` | Attribute | Flag indicating Non-Performing Loan threshold ($\text{DPD} \ge 90$). |

---

### 5. `hive.credit_risk.dim_application_decision`
- **Granularity**: 1 row per underwriting decision status and rejection reason.
- **HDFS Path**: `/curated/credit_risk/dim_application_decision`
- **Primary Key**: `sk_decision_key` (`Int` surrogate key)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_decision_key` | `INT` | PK | Surrogate primary key. |
| `name_contract_status` | `STRING` | Attribute | Decision outcome (`Approved`, `Refused`, `Canceled`, `Unused offer`). |
| `code_reject_reason` | `STRING` | Attribute | Underwriting decline code (`XAP`, `HC`, `LIMIT`, `SCO`, `CLIENT`, `SCOFR`, `SYSTEM`). |
| `name_client_type` | `STRING` | Attribute | Client relationship status (`Repeater`, `New`, `Refreshed`). |

---

### 6. `hive.credit_risk.dim_relative_time`
- **Granularity**: 1 row per integer day/month offset relative to application date.
- **HDFS Path**: `/curated/credit_risk/dim_relative_time`
- **Primary Key**: `sk_time_key` (`Int` surrogate key)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_time_key` | `INT` | PK | Surrogate key matching normalized integer offset. |
| `relative_day_offset` | `INT` | Attribute | Day offset ($0, -1, -30, -180, -365, \dots$). |
| `relative_month_offset` | `INT` | Attribute | Month offset ($0, -1, -3, -6, -12, -24, \dots$). |
| `relative_period_bucket` | `STRING` | Attribute | Time grouping (`Current / 0M`, `1-3M Ago`, `3-6M Ago`, `6-12M Ago`, `12-24M Ago`, `24M+ Ago`). |
| `vintage_cohort_offset` | `STRING` | Attribute | Quarterly vintage identifier relative to application window. |

---

## 5. Curated Fact Tables Reference & Schema

### 1. `hive.credit_risk.fact_loan_application`
- **Business Process**: Loan origination throughput, underwriting evaluation, pricing, risk scoring, and credit default tracking.
- **Fact Type**: **Transaction & Accumulating Snapshot Fact**.
- **Granularity**: 1 row per credit application (unifies `hive.stage_credit_risk.stage_previous_application` and `stage_application_train` / `stage_application_test`).
- **HDFS Path**: `/curated/credit_risk/fact_loan_application`
- **Storage Format**: Parquet / Snappy
- **Partition Column**: `product_group` (`STRING`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_application_key` | `BIGINT` | PK | Surrogate fact primary key. |
| `sk_id_curr` | `INT` | Degenerate | Customer natural key. |
| `sk_id_prev` | `INT` | Degenerate | Previous application natural key (NULL for current direct application). |
| `sk_customer_key` | `BIGINT` | FK | Foreign key to `hive.credit_risk.dim_customer`. |
| `sk_product_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_loan_product`. |
| `sk_channel_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_merchant_channel`. |
| `sk_decision_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_application_decision`. |
| `sk_time_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_relative_time`. |
| `amt_application` | `DECIMAL(18,2)` | Additive | Loan credit amount requested by applicant. |
| `amt_credit` | `DECIMAL(18,2)` | Additive | Final credit amount approved and disbursed. |
| `amt_annuity` | `DECIMAL(18,2)` | Additive | Monthly loan annuity installment amount. |
| `amt_goods_price` | `DECIMAL(18,2)` | Additive | Total price of goods financed (POS purchases). |
| `amt_down_payment` | `DECIMAL(18,2)` | Additive | Customer down payment amount. |
| `rate_down_payment` | `DECIMAL(8,6)` | Non-Additive | Down payment percentage of goods price. |
| `rate_interest_primary` | `DECIMAL(8,6)` | Non-Additive | Normalized contract interest rate. |
| `ext_source_1` | `FLOAT` | Non-Additive | External credit bureau risk score 1. |
| `ext_source_2` | `FLOAT` | Non-Additive | External credit bureau risk score 2. |
| `ext_source_3` | `FLOAT` | Non-Additive | External credit bureau risk score 3. |
| `target_default_flag` | `INT` | Semi-Additive | Default indicator (1: Default / payment difficulty, 0: All other cases, NULL: Test). |
| `is_current_application` | `BOOLEAN` | Attribute | True if current loan application, False if historical previous application. |

---

### 2. `hive.credit_risk.fact_installment_payment`
- **Business Process**: Granular repayment ledger transaction events, measuring repayment delays and collection shortfalls.
- **Fact Type**: **Transaction Fact**.
- **Granularity**: 1 row per scheduled payment installment per loan contract.
- **HDFS Path**: `/curated/credit_risk/fact_installment_payment`
- **Storage Format**: Parquet / Snappy
- **Partition Column**: `is_revolving_installment` (`BOOLEAN`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_installment_key` | `BIGINT` | PK | Surrogate fact primary key. |
| `sk_id_curr` | `INT` | Degenerate | Customer natural key. |
| `sk_id_prev` | `INT` | Degenerate | Loan contract natural key. |
| `sk_customer_key` | `BIGINT` | FK | Foreign key to `hive.credit_risk.dim_customer`. |
| `sk_product_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_loan_product`. |
| `sk_dpd_bucket_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_delinquency_bucket`. |
| `sk_time_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_relative_time` (due date offset). |
| `num_instalment_number` | `INT` | Attribute | Installment sequence number on contract schedule. |
| `num_instalment_version` | `INT` | Attribute | Schedule version (0: Credit Card / revolving payment, 1+: Term Loan). |
| `amt_instalment` | `DECIMAL(18,2)` | Additive | Scheduled installment payment amount. |
| `amt_payment` | `DECIMAL(18,2)` | Additive | Actual payment amount received. |
| `amt_underpayment` | `DECIMAL(18,2)` | Additive | Payment shortfall: $\max(0, \text{amt\_instalment} - \text{amt\_payment})$. |
| `payment_delay_days` | `INT` | Semi-Additive | Delay in payment: $(\text{days\_entry\_payment} - \text{days\_instalment})$. |
| `is_late_payment` | `INT` | Additive Count | 1 if payment made after due date, 0 otherwise. |
| `is_underpaid` | `INT` | Additive Count | 1 if actual payment < scheduled installment, 0 otherwise. |

---

### 3. `hive.credit_risk.fact_monthly_loan_snapshot`
- **Business Process**: Monthly longitudinal balance, credit limit, and delinquency aging tracking across POS and Credit Cards.
- **Fact Type**: **Periodic Snapshot Fact**.
- **Granularity**: 1 row per active loan contract per historical balance month (`MONTHS_BALANCE`).
- **HDFS Path**: `/curated/credit_risk/fact_monthly_loan_snapshot`
- **Storage Format**: Parquet / Snappy
- **Partition Column**: `loan_source_system` (`STRING`: `POS_CASH` vs `CREDIT_CARD`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_snapshot_key` | `BIGINT` | PK | Surrogate fact primary key. |
| `sk_id_curr` | `INT` | Degenerate | Customer natural key. |
| `sk_id_prev` | `INT` | Degenerate | Loan contract natural key. |
| `sk_customer_key` | `BIGINT` | FK | Foreign key to `hive.credit_risk.dim_customer`. |
| `sk_product_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_loan_product`. |
| `sk_dpd_bucket_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_delinquency_bucket`. |
| `sk_time_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_relative_time`. |
| `relative_month_offset` | `INT` | Attribute | Normalized `MONTHS_BALANCE` offset ($0, -1, -2, \dots$). |
| `loan_source_system` | `STRING` | Attribute | Source system identifier (`POS_CASH`, `CREDIT_CARD`). |
| `amt_balance` | `DECIMAL(18,2)` | Semi-Additive | Outstanding principal balance during the month. |
| `amt_credit_limit` | `DECIMAL(18,2)` | Semi-Additive | Actual credit card limit during the month. |
| `credit_utilization_ratio` | `DECIMAL(8,6)` | Non-Additive | Revolving utilization ratio: $\text{amt\_balance} / \text{amt\_credit\_limit}$. |
| `amt_drawings_current` | `DECIMAL(18,2)` | Additive | Total drawings during the month (ATM + POS merchant). |
| `amt_payment_current` | `DECIMAL(18,2)` | Additive | Total monthly repayment made by cardholder. |
| `cnt_instalment_total` | `INT` | Semi-Additive | Total contract term in months. |
| `cnt_instalment_future` | `INT` | Semi-Additive | Number of installments remaining to be paid. |
| `sk_dpd` | `INT` | Non-Additive | Days Past Due during the snapshot month. |
| `sk_dpd_def` | `INT` | Non-Additive | Days Past Due with tolerance for negligible overdue balances. |
| `contract_status` | `STRING` | Attribute | Monthly contract status (`Active`, `Completed`, `Signed`, `Demand`). |

---

### 4. `hive.credit_risk.fact_bureau_credit`
- **Business Process**: External credit lines opened by customers across other financial institutions reported to Credit Bureau.
- **Fact Type**: **Transaction & Accumulating Snapshot Fact**.
- **Granularity**: 1 row per external credit line in `bureau`.
- **HDFS Path**: `/curated/credit_risk/fact_bureau_credit`
- **Storage Format**: Parquet / Snappy

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_bureau_credit_key` | `BIGINT` | PK | Surrogate fact primary key. |
| `sk_id_curr` | `INT` | Degenerate | Customer natural key. |
| `sk_id_bureau` | `INT` | Degenerate | External credit bureau account identifier. |
| `sk_customer_key` | `BIGINT` | FK | Foreign key to `hive.credit_risk.dim_customer`. |
| `sk_bureau_source_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_bureau_source`. |
| `sk_time_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_relative_time`. |
| `credit_active_status` | `STRING` | Attribute | External credit status (`Closed`, `Active`, `Sold`, `Bad debt`). |
| `amt_credit_sum` | `DECIMAL(18,2)` | Additive | Total external credit amount granted. |
| `amt_credit_sum_debt` | `DECIMAL(18,2)` | Semi-Additive | Current outstanding debt on external credit. |
| `amt_credit_sum_limit` | `DECIMAL(18,2)` | Semi-Additive | Current credit limit on external credit card. |
| `amt_credit_sum_overdue` | `DECIMAL(18,2)` | Additive | Current overdue balance reported to bureau. |
| `amt_credit_max_overdue` | `DECIMAL(18,2)` | Non-Additive | Maximum overdue amount experienced during credit term. |
| `cnt_credit_prolong` | `INT` | Additive | Number of times external credit line was extended. |

---

### 5. `hive.credit_risk.fact_monthly_bureau_snapshot`
- **Business Process**: Longitudinal monthly delinquency status history for external credit lines reported to Credit Bureau.
- **Fact Type**: **Periodic Snapshot Fact**.
- **Granularity**: 1 row per external credit line per historical month in `bureau_balance`.
- **HDFS Path**: `/curated/credit_risk/fact_monthly_bureau_snapshot`
- **Storage Format**: Parquet / Snappy

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_bureau_snapshot_key` | `BIGINT` | PK | Surrogate fact primary key. |
| `sk_bureau_credit_key` | `BIGINT` | FK | Foreign key to `hive.credit_risk.fact_bureau_credit`. |
| `sk_dpd_bucket_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_delinquency_bucket`. |
| `sk_time_key` | `INT` | FK | Foreign key to `hive.credit_risk.dim_relative_time`. |
| `bureau_status_raw` | `STRING` | Attribute | Raw bureau code (`C`: Closed, `0`: 0 DPD, `1`..`5`: 30-120+ DPD, `X`: Unknown). |
| `is_closed` | `INT` | Additive Count | 1 if bureau status is `C` (Closed), 0 otherwise. |
| `is_overdue` | `INT` | Additive Count | 1 if status indicates delinquency (`1` through `5`), 0 otherwise. |

---

## 6. ClickHouse OLAP Serving Layer Design

ClickHouse acts as the high-throughput, low-latency analytical serving backend. It ingests partitioned data exported from the `hive.credit_risk.*` Gold layer and serves sub-second aggregations to BI dashboards and risk monitoring tools.

### 1. `default.ch_fact_loan_application` (Serving Fact Mart)
- **Table Engine**: `MergeTree()`
- **Sorting / Primary Key (`ORDER BY`)**: `(product_group, sk_product_key, sk_channel_key, sk_id_curr)`
- **Index Granularity**: 8,192 rows per index mark
- **Key Columns & Types**:
  - Primary & Foreign Keys: `sk_application_key` (`UInt64`), `sk_id_curr` (`UInt32`), `sk_customer_key` (`UInt64`), `sk_product_key` (`UInt16`), `sk_channel_key` (`UInt16`), `sk_decision_key` (`UInt16`), `relative_day_offset` (`Int32`).
  - Monetary Balances: `amt_application`, `amt_credit`, `amt_annuity`, `amt_goods_price`, `amt_down_payment` (`Decimal64(2)`).
  - Rates & Risk Scores: `rate_down_payment` (`Decimal32(6)`), `rate_interest_primary` (`Decimal32(6)`), `ext_source_1/2/3` (`Float32`).
  - Target & Partitioning: `target_default_flag` (`UInt8`), `is_current_application` (`UInt8`), `product_group` (`LowCardinality(String)`).

---

### 2. `default.agg_portfolio_delinquency_roll_rate` (Aggregated Risk Mart)
- **Business Goal**: Sub-10ms calculation of delinquency migration transition matrices ($\text{Current} \rightarrow \text{30 DPD} \rightarrow \text{60 DPD} \rightarrow \text{90+ DPD}$).
- **Table Engine**: `AggregatingMergeTree()`
- **Sorting / Primary Key (`ORDER BY`)**: `(relative_month_offset, loan_source_system, sk_product_key, prior_dpd_bucket, current_dpd_bucket)`
- **Key Dimensions**: `relative_month_offset` (`Int32`), `loan_source_system` (`LowCardinality(String)`), `sk_product_key` (`UInt16`), `prior_dpd_bucket` (`LowCardinality(String)`), `current_dpd_bucket` (`LowCardinality(String)`).
- **Stateful Aggregate Measures**:
  - `active_accounts_count`: `AggregateFunction(count, UInt32)`
  - `total_balance`: `AggregateFunction(sum, Decimal64(2))`
  - `total_overdue_balance`: `AggregateFunction(sum, Decimal64(2))`

---

### 3. `default.agg_vintage_loss_curves` (Vintage Performance Mart)
- **Business Goal**: Real-time cohort default rate tracking by disbursement quarter across loan lifecycle ($MOB = \text{Month on Book}$).
- **Table Engine**: `AggregatingMergeTree()`
- **Sorting / Primary Key (`ORDER BY`)**: `(origination_cohort, months_on_book, product_group)`
- **Key Dimensions**: `origination_cohort` (`LowCardinality(String)`), `months_on_book` (`UInt16`), `product_group` (`LowCardinality(String)`).
- **Stateful Aggregate Measures**:
  - `disbursed_loan_count`: `AggregateFunction(count, UInt32)`
  - `total_disbursed_amount`: `AggregateFunction(sum, Decimal64(2))`
  - `cumulative_default_count`: `AggregateFunction(sum, UInt8)`
  - `cumulative_default_amount`: `AggregateFunction(sum, Decimal64(2))`

---

## 7. Storage, Partitioning & ClickHouse Optimization Guidelines

1. **Strict Decimal Money Precision**:
   - In accordance with enterprise financial engineering standards, all monetary fields (`amt_application`, `amt_credit`, `amt_balance`, `amt_payment`) utilize `Decimal(18,2)` in Spark/Hive and `Decimal64(2)` in ClickHouse. Floating-point types (`Float`/`Double`) are strictly prohibited for balances to prevent cumulative rounding errors.

2. **ClickHouse Sparse Index & `ORDER BY` Alignment**:
   - Tables are ordered starting with low-cardinality filtering columns (`product_group`, `loan_source_system`, `relative_month_offset`) followed by high-cardinality join keys (`sk_id_curr`). This guarantees maximum compression and allows ClickHouse to skip hundreds of megabytes during query execution.

3. **LowCardinality Optimization**:
   - String dimensions with cardinality $< 10,000$ (e.g. `product_group`, `bucket_code`, `loan_source_system`, `channel_type`) are encoded as `LowCardinality(String)`, storing 1-byte integer dictionary references for multi-gigabyte scans.

4. **Codecs & Data Compression**:
   - High-precision timestamp/day offsets utilize `DoubleDelta` or `T64` codecs.
   - Numeric and string metric columns utilize `ZSTD(3)` compression for optimal IO throughput.

---

## 8. End-to-End Data Lineage Matrix (Raw $\rightarrow$ Stage $\rightarrow$ Curated)

| Raw Layer (`hive.raw_credit_risk.*`) | Stage Layer (`hive.stage_credit_risk.*`) | Curated Layer (`hive.credit_risk.*`) | Transformation & Business Rules |
| :--- | :--- | :--- | :--- |
| `raw_application_train`<br>`raw_application_test` | `stage_application_train`<br>`stage_application_test` | `dim_customer` | Deduplicate on `SK_ID_CURR`, standardize gender and demographic strings, compute derived `age_years` and `employed_years`, assign `sk_customer_key`. |
| `raw_application_train`<br>`raw_previous_application` | `stage_application_train`<br>`stage_previous_application` | `dim_loan_product` | Extract distinct contract types (`Cash loans`, `Revolving loans`, `Consumer loans`), map portfolio group and revolving flags. |
| `raw_previous_application` | `stage_previous_application` | `dim_merchant_channel` | Extract unique combinations of `CHANNEL_TYPE`, `NAME_GOODS_CATEGORY`, `NAME_SELLER_INDUSTRY`, and `NAME_YIELD_GROUP`. |
| *Lookup / Risk Rule Master* | *Standard Risk Code Master* | `dim_delinquency_bucket` | Standardize DPD aging brackets (0 DPD, 1-30 DPD, 31-60 DPD, 61-90 DPD, 90+ DPD / NPL). |
| `raw_previous_application` | `stage_previous_application` | `dim_application_decision` | Map approval and rejection codes (`Approved`, `Refused`, `Canceled`) and rejection reason codes (`CODE_REJECT_REASON`). |
| `raw_application_train`<br>`raw_previous_application` | `stage_application_train`<br>`stage_previous_application` | `fact_loan_application` | Cast monetary amounts to `Decimal(18,2)`, map foreign keys to conformed dimensions, unify historical previous applications with current application decisions. |
| `raw_installments_payments` | `stage_installments_payments` | `fact_installment_payment` | Clean nulls, calculate payment delay days (`DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT`) and shortfall amount ($\max(0, \text{AMT\_INSTALMENT} - \text{AMT\_PAYMENT})$). |
| `raw_pos_cash_balance`<br>`raw_credit_card_balance` | `stage_pos_cash_balance`<br>`stage_credit_card_balance` | `fact_monthly_loan_snapshot` | Normalize monthly contract snapshots, compute revolving credit utilization ratio ($\text{balance} / \text{limit}$), map delinquency bucket foreign keys. |
| `raw_bureau` | `stage_bureau` | `fact_bureau_credit` | Standardize active/closed status, cast external debt balances, overdue amounts, and prolong counts to `Decimal(18,2)`. |
| `raw_bureau_balance` | `stage_bureau_balance` | `fact_monthly_bureau_snapshot` | Map monthly external bureau status codes (`C`, `0`, `1`..`5`) to conformed delinquency buckets and closed flags. |

---

## 9. Financial Data Integrity & Reconciliation Controls

In production lending platforms, automated data quality gates must enforce financial reconciliations across the ETL boundary between Stage (`hive.stage_credit_risk.*`) and Curated (`hive.credit_risk.*`):

1. **Conservation of Financial Volume (Amount-Sum Check)**:
   $$\sum \text{amt\_instalment}_{\text{curated}} \equiv \sum \text{AMT\_INSTALMENT}_{\text{stage}} \quad (\pm 0.00 \text{ discrepancy})$$
2. **Referential Integrity on Foreign Keys**:
   - Zero tolerance for orphaned fact records (`sk_customer_key IS NULL` or `sk_product_key IS NULL`). All unmapped source values default to standard `Unknown (-1)` surrogate dimension keys.
3. **Partition Overwrite Idempotency**:
   - Spark write jobs enforce `spark.sql.sources.partitionOverwriteMode=dynamic` ensuring backfills replace only target analytical partitions without table-level locking or record loss.
