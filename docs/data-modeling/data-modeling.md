# Home Credit Default Risk: Data Modeling Guide

## 1. Overview & Project Goals

In this project, I build a data pipeline on Hadoop (HDFS, YARN, Spark, Hive) to process the Home Credit Default Risk dataset. The main goal is to prepare clean data for two purposes:
1. **Business Reports & Risk Dashboards**: Tracking delinquency rates, default rates, loan approval outcomes, and portfolio metrics.
2. **Feature Marts for Machine Learning**: Creating aggregated customer features to train credit scoring models.

### Data Layers in the Pipeline

I organize the data into three layers on HDFS before exporting to ClickHouse:
- **Raw Layer (`hive.raw_credit_risk.*`)**: Ingests original CSV files directly into Parquet format on HDFS (`/raw/credit_risk/*`). I add an `_ingested_at` column and keep the original data unchanged.
- **Stage Layer (`hive.stage_credit_risk.*`)**: Cleans the data, fixes data types (such as `Decimal(18,2)` for monetary values), removes duplicate rows using primary keys, and stores cleaned tables on HDFS (`/stage/credit_risk/*`).
- **Curated Layer (`hive.credit_risk.*`)**: Implements a dimensional model based on the Kimball approach (using a Constellation / Galaxy Schema with shared dimensions). Data is stored in Parquet format on HDFS (`/curated/credit_risk/*`).
- **Serving Layer (`ClickHouse`)**: Loads final fact tables and summary marts into ClickHouse `MergeTree` tables for fast BI reporting and dashboard queries.

### Key Design Choices

1. **Cleaned Source Tables in Stage**:
   - In the Stage layer, I keep tables 1:1 with source files (8 clean tables).
   - I keep the original business keys (`sk_id_curr`, `sk_id_prev`, `sk_id_bureau`).
   - I do not build dimensions or facts in Stage. Keeping Stage close to the source makes data easy to inspect and debug, avoids doing modeling work twice, and keeps the pipeline simple.

2. **Surrogate Keys using `xxhash64`**:
   - Instead of using auto-increment numbers or Spark's `monotonically_increasing_id()`, I generate 64-bit integer surrogate keys using `xxhash64(concat_ws('||', ...))`.
   - Reason: It is fast, works in parallel across Spark worker nodes without cluster locks or shuffle overhead, and always produces the exact same key when re-running the job (idempotent).

3. **Unknown Dimension Record (`-1` Fallback)**:
   - For every dimension table, I insert one default record with `SK = -1`, `Natural Key = -1`, string values set to `'Unknown'`, and dates set to `'1900-01-01'`.
   - When building fact tables, if a dimension lookup does not find a match, I use `coalesce(dim_sk, -1)`.
   - Reason: This prevents dropping rows when running `INNER JOIN` in queries or BI tools if dimension records are missing or arrive late.

4. **Using `Decimal(18,2)` for Money**:
   - I use `Decimal(18,2)` in Spark/Hive and `Decimal64(2)` in ClickHouse for all currency columns (`amt_credit`, `amt_balance`, `amt_payment`, etc.) to prevent floating-point rounding errors.

---

## 2. Bus Matrix

The Bus Matrix shows how conformed dimensions are shared across the fact tables in the Curated layer:

| Business Process / Fact Table | Grain | Fact Type | `dim_customer` | `dim_loan_product` | `dim_merchant_channel` | `dim_bureau_source` | `dim_delinquency_bucket` | `dim_application_decision` | `dim_relative_time` |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`hive.credit_risk.fact_loan_application`** | 1 row per credit application | Transaction / Snapshot | **X** | **X** | **X** | | | **X** | **X** |
| **`hive.credit_risk.fact_installment_payment`** | 1 row per payment installment | Transaction | **X** | **X** | | | **X** | | **X** |
| **`hive.credit_risk.fact_monthly_loan_snapshot`** | 1 row per active loan per month | Periodic Snapshot | **X** | **X** | **X** | | **X** | | **X** |
| **`hive.credit_risk.fact_bureau_credit`** | 1 row per external credit line | Transaction | **X** | | | **X** | | | **X** |
| **`hive.credit_risk.fact_monthly_bureau_snapshot`** | 1 row per external credit line per month | Periodic Snapshot | **X** | | | **X** | **X** | | **X** |
| **`hive.credit_risk.obt_loan_portfolio_360`** | 1 row per application (Denormalized) | Wide BI Table | **X** | **X** | **X** | | | **X** | |

---

## 3. Dimension-to-Fact Relationships

This table shows how each dimension links to child fact tables, along with the join key and fallback behavior:

| Dimension (`hive.credit_risk`) | Fact Table (`hive.credit_risk`) | Foreign Key | Cardinality | Fallback Rule | Description |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `dim_customer` | `fact_loan_application` | `sk_customer_key` | $1 : N$ | `-1` | Customer demographic and risk profile at application time. |
| `dim_loan_product` | `fact_loan_application` | `sk_product_key` | $1 : N$ | `-1` | Product type (Cash loans, Revolving loans, POS loans). |
| `dim_merchant_channel` | `fact_loan_application` | `sk_channel_key` | $1 : N$ | `-1` | Partner store channel, seller industry, and goods category. |
| `dim_application_decision` | `fact_loan_application` | `sk_decision_key` | $1 : N$ | `-1` | Approval or rejection decision outcome and reason. |
| `dim_relative_time` | `fact_loan_application` | `sk_time_key` | $1 : N$ | `-1` | Relative day/month offset from application time. |
| `dim_customer` | `fact_installment_payment` | `sk_customer_key` | $1 : N$ | `-1` | Customer making the scheduled or actual repayment. |
| `dim_loan_product` | `fact_installment_payment` | `sk_product_key` | $1 : N$ | `-1` | Loan product linked to the installment. |
| `dim_delinquency_bucket` | `fact_installment_payment` | `sk_dpd_bucket_key` | $1 : N$ | `-1` | Delinquency aging bracket for delayed payments. |
| `dim_relative_time` | `fact_installment_payment` | `sk_time_key` | $1 : N$ | `-1` | Relative installment due date offset. |
| `dim_customer` | `fact_monthly_loan_snapshot` | `sk_customer_key` | $1 : N$ | `-1` | Customer holding active credit account. |
| `dim_loan_product` | `fact_monthly_loan_snapshot` | `sk_product_key` | $1 : N$ | `-1` | Product type (POS Cash loan vs Revolving Card). |
| `dim_delinquency_bucket` | `fact_monthly_loan_snapshot` | `sk_dpd_bucket_key` | $1 : N$ | `-1` | Monthly DPD classification (Current, 30 DPD, 90+ DPD). |
| `dim_relative_time` | `fact_monthly_loan_snapshot` | `sk_time_key` | $1 : N$ | `-1` | Snapshot month offset (`MONTHS_BALANCE`). |
| `dim_customer` | `fact_bureau_credit` | `sk_customer_key` | $1 : N$ | `-1` | External credit lines opened with other institutions. |
| `dim_bureau_source` | `fact_bureau_credit` | `sk_bureau_source_key` | $1 : N$ | `-1` | External credit type and collateral indicator. |
| `dim_relative_time` | `fact_bureau_credit` | `sk_time_key` | $1 : N$ | `-1` | Relative credit opening date offset. |
| `fact_bureau_credit` | `fact_monthly_bureau_snapshot` | `sk_bureau_credit_key` | $1 : N$ | `-1` | Monthly tracking of external bureau credit accounts. |
| `dim_delinquency_bucket` | `fact_monthly_bureau_snapshot` | `sk_dpd_bucket_key` | $1 : N$ | `-1` | Bureau DPD status code mapped to delinquency bracket. |
| `dim_relative_time` | `fact_monthly_bureau_snapshot` | `sk_time_key` | $1 : N$ | `-1` | Monthly historical snapshot window. |

---

## 4. Curated Dimension Tables Reference

### 1. `hive.credit_risk.dim_customer`
- **Granularity**: 1 row per customer (supports SCD Type 2 history).
- **HDFS Path**: `/curated/credit_risk/dim_customer`
- **Primary Key**: `sk_customer_key` (`BIGINT`, generated using `xxhash64(sk_id_curr, valid_from)`)
- **Business Key**: `sk_id_curr` (`INT` from `stage_application_train` / `stage_application_test`)
- **SCD Policy**: Type 2 for changes in income, family status, and housing; Type 1 for basic demographics.
- **Default Record**: `sk_customer_key = -1`, `sk_id_curr = -1`, string columns set to `'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_customer_key` | `BIGINT` | PK | Surrogate key generated with `xxhash64`. |
| `sk_id_curr` | `INT` | BK | Original customer ID from source data. |
| `code_gender` | `STRING` | Attribute | Customer gender (`M`, `F`, `XNA`, `Unknown`). |
| `flag_own_car` | `STRING` | Attribute | Car ownership flag (`Y`, `N`). |
| `flag_own_realty` | `STRING` | Attribute | Real estate ownership flag (`Y`, `N`). |
| `cnt_children` | `INT` | Attribute | Number of children. |
| `cnt_fam_members` | `FLOAT` | Attribute | Total count of family members. |
| `amt_income_total` | `DECIMAL(18,2)` | Attribute | Total income amount. |
| `name_income_type` | `STRING` | Attribute | Income source category (`Working`, `Commercial associate`, etc.). |
| `name_education_type` | `STRING` | Attribute | Education level. |
| `name_family_status` | `STRING` | Attribute | Marital and family status. |
| `name_housing_type` | `STRING` | Attribute | Housing situation (`House / apartment`, `Rented`, etc.). |
| `occupation_type` | `STRING` | Attribute | Occupation category (`Laborers`, `Sales staff`, etc.). |
| `organization_type` | `STRING` | Attribute | Employer industry. |
| `age_years` | `INT` | Derived | Computed customer age: $\text{floor}(\text{DAYS\_BIRTH} / -365.25)$. |
| `employed_years` | `INT` | Derived | Computed work experience: $\text{floor}(\text{DAYS\_EMPLOYED} / -365.25)$. |
| `valid_from` | `TIMESTAMP` | SCD Meta | Effective start time of this record version. |
| `valid_to` | `TIMESTAMP` | SCD Meta | Effective end time (`9999-12-31 23:59:59` for active record). |
| `is_current` | `BOOLEAN` | SCD Meta | True for the latest active record version. |

---

### 2. `hive.credit_risk.dim_loan_product`
- **Granularity**: 1 row per loan product.
- **HDFS Path**: `/curated/credit_risk/dim_loan_product`
- **Primary Key**: `sk_product_key` (`INT`, deterministic integer hash)
- **Default Record**: `sk_product_key = -1`, `name_contract_type = 'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_product_key` | `INT` | PK | Surrogate key for the loan product. |
| `name_contract_type` | `STRING` | Attribute | Contract type name (`Cash loans`, `Revolving loans`, `Consumer loans`). |
| `portfolio_category` | `STRING` | Attribute | Product group (`Secured`, `Unsecured Term`, `Revolving Credit`). |
| `product_group` | `STRING` | Attribute | Simplified product group (`Personal Cash`, `POS Line`, `Credit Card`). |
| `is_revolving` | `BOOLEAN` | Attribute | True if product is a revolving credit facility. |

---

### 3. `hive.credit_risk.dim_merchant_channel`
- **Granularity**: 1 row per merchant sales channel and goods category combination.
- **HDFS Path**: `/curated/credit_risk/dim_merchant_channel`
- **Primary Key**: `sk_channel_key` (`INT`, deterministic integer hash)
- **Default Record**: `sk_channel_key = -1`, `channel_type = 'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_channel_key` | `INT` | PK | Surrogate key for the merchant channel. |
| `channel_type` | `STRING` | Attribute | Channel name (`Country-wide`, `Contact center`, `Stone`, etc.). |
| `name_type_suite` | `STRING` | Attribute | Accompaniment suite (`Unaccompanied`, `Family`, etc.). |
| `name_goods_category` | `STRING` | Attribute | Asset category financed (`Mobile`, `Computers`, `Auto`, etc.). |
| `name_seller_industry` | `STRING` | Attribute | Seller industry (`Consumer electronics`, `Furniture`, etc.). |
| `name_yield_group` | `STRING` | Attribute | Pricing tier group (`low_normal`, `middle`, `high`, `low_action`). |

---

### 4. `hive.credit_risk.dim_delinquency_bucket`
- **Granularity**: 1 row per standard delinquency aging bracket.
- **HDFS Path**: `/curated/credit_risk/dim_delinquency_bucket`
- **Primary Key**: `sk_dpd_bucket_key` (`INT`, deterministic integer hash)
- **Default Record**: `sk_dpd_bucket_key = -1`, `bucket_code = 'UNKNOWN'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_dpd_bucket_key` | `INT` | PK | Surrogate key for the delinquency bracket. |
| `bucket_code` | `STRING` | Attribute | Bracket code (`B0`, `B1`, `B2`, `B3`, `B4`, `B5`, `NPL`). |
| `bucket_name` | `STRING` | Attribute | Display name (`Current / 0 DPD`, `1-30 DPD`, `90+ DPD / NPL`). |
| `dpd_min` | `INT` | Attribute | Minimum Days Past Due bound (inclusive). |
| `dpd_max` | `INT` | Attribute | Maximum Days Past Due bound (inclusive, `99999` for highest). |
| `is_npl` | `BOOLEAN` | Attribute | Flag indicating Non-Performing Loan threshold ($\text{DPD} \ge 90$). |

---

### 5. `hive.credit_risk.dim_application_decision`
- **Granularity**: 1 row per underwriting decision status and rejection reason.
- **HDFS Path**: `/curated/credit_risk/dim_application_decision`
- **Primary Key**: `sk_decision_key` (`INT`, deterministic integer hash)
- **Default Record**: `sk_decision_key = -1`, `name_contract_status = 'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_decision_key` | `INT` | PK | Surrogate key for underwriting decision. |
| `name_contract_status` | `STRING` | Attribute | Outcome status (`Approved`, `Refused`, `Canceled`, `Unused offer`). |
| `code_reject_reason` | `STRING` | Attribute | Rejection reason code (`XAP`, `HC`, `LIMIT`, `SCO`, `CLIENT`, `SYSTEM`). |
| `name_client_type` | `STRING` | Attribute | Customer status at application time (`Repeater`, `New`, `Refreshed`). |

---

### 6. `hive.credit_risk.dim_relative_time`
- **Granularity**: 1 row per relative day/month offset.
- **HDFS Path**: `/curated/credit_risk/dim_relative_time`
- **Primary Key**: `sk_time_key` (`INT`, normalized integer offset)
- **Default Record**: `sk_time_key = -1`, `relative_period_bucket = 'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_time_key` | `INT` | PK | Normalized integer offset value. |
| `relative_day_offset` | `INT` | Attribute | Offset in days ($0, -1, -30, -180, -365, \dots$). |
| `relative_month_offset` | `INT` | Attribute | Offset in months ($0, -1, -3, -6, -12, -24, \dots$). |
| `relative_period_bucket` | `STRING` | Attribute | Time bucket group (`Current / 0M`, `1-3M Ago`, `12-24M Ago`, etc.). |
| `vintage_cohort_offset` | `STRING` | Attribute | Quarterly cohort label relative to application date. |

---

### 7. `hive.credit_risk.dim_bureau_source`
- **Granularity**: 1 row per external credit product category.
- **HDFS Path**: `/curated/credit_risk/dim_bureau_source`
- **Primary Key**: `sk_bureau_source_key` (`INT`, deterministic integer hash)
- **Default Record**: `sk_bureau_source_key = -1`, `credit_type = 'Unknown'`.

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_bureau_source_key` | `INT` | PK | Surrogate key for external credit type. |
| `credit_type` | `STRING` | Attribute | External credit type (`Consumer credit`, `Credit card`, `Mortgage`, etc.). |
| `credit_category` | `STRING` | Attribute | High-level category (`Revolving`, `Installment`, `Secured`, `Other`). |
| `is_secured` | `BOOLEAN` | Attribute | True if credit has collateral security. |

---

## 5. Curated Fact Tables Reference

### 1. `hive.credit_risk.fact_loan_application`
- **Granularity**: 1 row per loan application (combines previous applications and current train/test applications).
- **HDFS Path**: `/curated/credit_risk/fact_loan_application`
- **Partition Column**: `product_group` (`STRING`)
- **Primary Key**: `sk_application_key` (`BIGINT`, generated using `xxhash64(sk_id_curr, sk_id_prev, is_current_application)`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_application_key` | `BIGINT` | PK | Surrogate primary key (`xxhash64`). |
| `sk_id_curr` | `INT` | Degenerate | Customer original ID. |
| `sk_id_prev` | `INT` | Degenerate | Previous loan application ID (0 for direct application). |
| `sk_customer_key` | `BIGINT` | FK | Link to `dim_customer` (falls back to `-1`). |
| `sk_product_key` | `INT` | FK | Link to `dim_loan_product` (falls back to `-1`). |
| `sk_channel_key` | `INT` | FK | Link to `dim_merchant_channel` (falls back to `-1`). |
| `sk_decision_key` | `INT` | FK | Link to `dim_application_decision` (falls back to `-1`). |
| `sk_time_key` | `INT` | FK | Link to `dim_relative_time` (falls back to `-1`). |
| `name_contract_type` | `STRING` | Degenerate | Contract type name. |
| `amt_application` | `DECIMAL(18,2)` | Metric | Loan amount requested by applicant. |
| `amt_credit` | `DECIMAL(18,2)` | Metric | Final credit amount approved. |
| `amt_annuity` | `DECIMAL(18,2)` | Metric | Monthly annuity payment amount. |
| `amt_goods_price` | `DECIMAL(18,2)` | Metric | Price of financed goods. |
| `amt_down_payment` | `DECIMAL(18,2)` | Metric | Customer down payment amount. |
| `rate_down_payment` | `DECIMAL(8,6)` | Metric | Down payment percentage of goods price. |
| `rate_interest_primary` | `DECIMAL(8,6)` | Metric | Primary interest rate. |
| `ext_source_1` | `FLOAT` | Metric | External credit score 1. |
| `ext_source_2` | `FLOAT` | Metric | External credit score 2. |
| `ext_source_3` | `FLOAT` | Metric | External credit score 3. |
| `target_default_flag` | `INT` | Metric | Default label (1: Default, 0: Non-default, NULL: Test). |
| `is_current_application` | `BOOLEAN` | Attribute | True for current application, False for previous loan. |
| `product_group` | `STRING` | Partition | Partition column (`Personal Cash`, `POS Line`, `Credit Card`). |

---

### 2. `hive.credit_risk.fact_installment_payment`
- **Granularity**: 1 row per scheduled payment installment.
- **HDFS Path**: `/curated/credit_risk/fact_installment_payment`
- **Partition Column**: `is_revolving_installment` (`BOOLEAN`)
- **Primary Key**: `sk_installment_key` (`BIGINT`, generated using `xxhash64(sk_id_prev, num_instalment_number, num_instalment_version, days_instalment)`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_installment_key` | `BIGINT` | PK | Surrogate primary key (`xxhash64`). |
| `sk_id_prev` | `INT` | Degenerate | Previous loan application ID. |
| `sk_id_curr` | `INT` | Degenerate | Customer original ID. |
| `sk_customer_key` | `BIGINT` | FK | Link to `dim_customer` (falls back to `-1`). |
| `sk_product_key` | `INT` | FK | Link to `dim_loan_product` (falls back to `-1`). |
| `sk_dpd_bucket_key` | `INT` | FK | Link to `dim_delinquency_bucket` (falls back to `-1`). |
| `sk_time_key` | `INT` | FK | Link to `dim_relative_time` (due date offset). |
| `num_instalment_version` | `INT` | Attribute | Installment version (0: Credit Card, 1+: Term Loan). |
| `num_instalment_number` | `INT` | Attribute | Installment sequence number. |
| `days_instalment` | `INT` | Attribute | Scheduled payment due date offset. |
| `days_entry_payment` | `INT` | Attribute | Actual payment date offset. |
| `amt_instalment` | `DECIMAL(18,2)` | Metric | Scheduled installment amount. |
| `amt_payment` | `DECIMAL(18,2)` | Metric | Actual amount paid. |
| `amt_underpayment` | `DECIMAL(18,2)` | Metric | Shortfall: $\max(0, \text{amt\_instalment} - \text{amt\_payment})$. |
| `payment_delay_days` | `INT` | Metric | Days late: $(\text{days\_entry\_payment} - \text{days\_instalment})$. |
| `is_late_payment` | `INT` | Metric | 1 if paid after due date, 0 otherwise. |
| `is_underpaid` | `INT` | Metric | 1 if payment < scheduled amount, 0 otherwise. |
| `is_revolving_installment` | `BOOLEAN` | Partition | Partition flag (True for cards, False for term loans). |

---

### 3. `hive.credit_risk.fact_monthly_loan_snapshot`
- **Granularity**: 1 row per active loan per historical month (`MONTHS_BALANCE`).
- **HDFS Path**: `/curated/credit_risk/fact_monthly_loan_snapshot`
- **Partition Column**: `loan_source_system` (`STRING`: `POS_CASH` vs `CREDIT_CARD`)
- **Primary Key**: `sk_snapshot_key` (`BIGINT`, generated using `xxhash64(sk_id_prev, relative_month_offset, loan_source_system)`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_snapshot_key` | `BIGINT` | PK | Surrogate primary key (`xxhash64`). |
| `sk_id_prev` | `INT` | Degenerate | Previous loan application ID. |
| `sk_id_curr` | `INT` | Degenerate | Customer original ID. |
| `sk_customer_key` | `BIGINT` | FK | Link to `dim_customer` (falls back to `-1`). |
| `sk_product_key` | `INT` | FK | Link to `dim_loan_product` (falls back to `-1`). |
| `sk_dpd_bucket_key` | `INT` | FK | Link to `dim_delinquency_bucket` (falls back to `-1`). |
| `sk_time_key` | `INT` | FK | Link to `dim_relative_time` (falls back to `-1`). |
| `relative_month_offset` | `INT` | Attribute | Monthly offset ($0, -1, -2, \dots$). |
| `amt_balance` | `DECIMAL(18,2)` | Metric | Outstanding balance in the month. |
| `amt_credit_limit` | `DECIMAL(18,2)` | Metric | Credit card limit in the month. |
| `credit_utilization_ratio` | `DECIMAL(8,6)` | Metric | Utilization: $\text{amt\_balance} / \text{amt\_credit\_limit}$. |
| `amt_drawings_current` | `DECIMAL(18,2)` | Metric | Total drawings during the month. |
| `amt_payment_current` | `DECIMAL(18,2)` | Metric | Total payments made in the month. |
| `cnt_instalment_total` | `INT` | Metric | Total term of contract in months. |
| `cnt_instalment_future` | `INT` | Metric | Remaining installments to pay. |
| `sk_dpd` | `INT` | Metric | Days Past Due in the month. |
| `sk_dpd_def` | `INT` | Metric | Days Past Due with tolerance for small debts. |
| `contract_status` | `STRING` | Attribute | Contract status (`Active`, `Completed`, `Signed`, etc.). |
| `loan_source_system` | `STRING` | Partition | Source system partition (`POS_CASH`, `CREDIT_CARD`). |

---

### 4. `hive.credit_risk.fact_bureau_credit`
- **Granularity**: 1 row per external credit line from credit bureau data.
- **HDFS Path**: `/curated/credit_risk/fact_bureau_credit`
- **Primary Key**: `sk_bureau_credit_key` (`BIGINT`, generated using `xxhash64(sk_id_bureau, sk_id_curr)`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_bureau_credit_key` | `BIGINT` | PK | Surrogate primary key (`xxhash64`). |
| `sk_id_bureau` | `INT` | Degenerate | External credit bureau account ID. |
| `sk_id_curr` | `INT` | Degenerate | Customer original ID. |
| `sk_customer_key` | `BIGINT` | FK | Link to `dim_customer` (falls back to `-1`). |
| `sk_bureau_source_key` | `INT` | FK | Link to `dim_bureau_source` (falls back to `-1`). |
| `sk_time_key` | `INT` | FK | Link to `dim_relative_time` (falls back to `-1`). |
| `credit_active_status` | `STRING` | Attribute | Status (`Closed`, `Active`, `Sold`, `Bad debt`). |
| `credit_type` | `STRING` | Degenerate | External product type name. |
| `days_credit` | `INT` | Attribute | Days before current application credit was opened. |
| `credit_day_overdue` | `INT` | Attribute | Overdue days on external credit. |
| `days_credit_enddate` | `INT` | Attribute | Remaining credit duration in days. |
| `days_enddate_fact` | `INT` | Attribute | Days since external credit was closed. |
| `cnt_credit_prolong` | `INT` | Metric | Number of times credit line was extended. |
| `amt_credit_sum` | `DECIMAL(18,2)` | Metric | Total credit amount granted. |
| `amt_credit_sum_debt` | `DECIMAL(18,2)` | Metric | Current debt outstanding. |
| `amt_credit_sum_limit` | `DECIMAL(18,2)` | Metric | Current credit card limit. |
| `amt_credit_sum_overdue` | `DECIMAL(18,2)` | Metric | Current overdue balance. |
| `amt_credit_max_overdue` | `DECIMAL(18,2)` | Metric | Max overdue amount seen on this loan. |

---

### 5. `hive.credit_risk.fact_monthly_bureau_snapshot`
- **Granularity**: 1 row per external credit line per historical month.
- **HDFS Path**: `/curated/credit_risk/fact_monthly_bureau_snapshot`
- **Primary Key**: `sk_bureau_snapshot_key` (`BIGINT`, generated using `xxhash64(sk_id_bureau, relative_month_offset)`)

| Column Name | Data Type | Key Type | Description |
| :--- | :--- | :--- | :--- |
| `sk_bureau_snapshot_key` | `BIGINT` | PK | Surrogate primary key (`xxhash64`). |
| `sk_bureau_credit_key` | `BIGINT` | FK | Link to `fact_bureau_credit` (falls back to `-1`). |
| `sk_dpd_bucket_key` | `INT` | FK | Link to `dim_delinquency_bucket` (falls back to `-1`). |
| `sk_time_key` | `INT` | FK | Link to `dim_relative_time` (falls back to `-1`). |
| `sk_id_bureau` | `INT` | Degenerate | External bureau account ID. |
| `relative_month_offset` | `INT` | Attribute | Monthly offset ($0, -1, -2, \dots$). |
| `bureau_status_raw` | `STRING` | Attribute | Original status code (`C`, `0`, `1`..`5`, `X`). |
| `is_closed` | `INT` | Metric | 1 if status is `C` (Closed), 0 otherwise. |
| `is_overdue` | `INT` | Metric | 1 if status indicates overdue (`1` to `5`), 0 otherwise. |

---

## 6. Curated One Big Table (OBT) & Feature Marts

To make reporting and machine learning easy without requiring complex multi-table SQL queries, I build:

### 1. `hive.credit_risk.obt_loan_portfolio_360` (Wide BI Table)
- **Granularity**: 1 row per loan application.
- **HDFS Path**: `/curated/credit_risk/obt_loan_portfolio_360`
- **Description**: A denormalized table pre-joining `fact_loan_application` with customer demographics, loan product, merchant channel, and application decision dimensions. BI dashboards can query this table directly without joining multiple tables.
- **Key Columns**:
  - IDs & Labels: `sk_id_curr`, `sk_id_prev`, `is_current_application`, `target_default_flag`.
  - Customer Information: `code_gender`, `amt_income_total`, `name_income_type`, `name_education_type`, `age_years`, `employed_years`.
  - Product & Merchant: `name_contract_type`, `portfolio_category`, `product_group`, `channel_type`, `name_goods_category`.
  - Decision Status: `name_contract_status`, `code_reject_reason`, `name_client_type`.
  - Financial Metrics: `amt_application`, `amt_credit`, `amt_annuity`, `amt_goods_price`, `amt_down_payment`, `rate_down_payment`.
  - Risk Scores: `ext_source_1`, `ext_source_2`, `ext_source_3`.

---

## 7. ClickHouse Serving Tables

ClickHouse provides fast query responses for BI dashboards. I export curated facts and aggregations to ClickHouse:

### 1. `default.ch_fact_loan_application`
- **Table Engine**: `MergeTree()`
- **Order By**: `(product_group, sk_product_key, sk_channel_key, sk_id_curr)`
- **Purpose**: Fast filtering and slice-and-dice queries on loan applications by product group and customer.

### 2. `default.agg_portfolio_delinquency_roll_rate`
- **Table Engine**: `AggregatingMergeTree()`
- **Order By**: `(relative_month_offset, loan_source_system, sk_product_key, prior_dpd_bucket, current_dpd_bucket)`
- **Purpose**: Computes transition rates between delinquency stages ($\text{Current} \rightarrow \text{30 DPD} \rightarrow \text{60 DPD} \rightarrow \text{90+ DPD}$).

### 3. `default.agg_vintage_loss_curves`
- **Table Engine**: `AggregatingMergeTree()`
- **Order By**: `(origination_cohort, months_on_book, product_group)`
- **Purpose**: Tracks cumulative default rates by loan disbursement quarterly cohort over time.

---

## 8. Storage, Partitioning & Precision Guidelines

1. **Decimal Precision for Money**:
   - Always use `Decimal(18,2)` in Spark/Hive and `Decimal64(2)` in ClickHouse for money columns (`amt_credit`, `amt_balance`, `amt_payment`). Do not use `Float` or `Double` for financial balances.

2. **Surrogate Keys with `xxhash64`**:
   - Generating keys with `abs(xxhash64(...))` runs fast across partitions, does not require sorting, and always produces the same key for the same input row.

3. **Fallback to `-1` for Unknown Records**:
   - Default `-1` keys ensure queries using `INNER JOIN` in BI tools will not drop rows when dimension data is missing or not yet loaded.

4. **ClickHouse Data Types**:
   - Use `LowCardinality(String)` for category columns with fewer than 10,000 distinct values (`product_group`, `channel_type`, `bucket_code`).
   - Use `ZSTD(3)` compression for numbers and strings to save disk space and improve read speeds.

---

## 9. End-to-End Data Lineage Table

The table below outlines how data moves from Raw source files to Cleaned Stage tables, and finally into Curated dimensions and facts:

| Raw Layer (`hive.raw_credit_risk.*`) | Stage Layer (`hive.stage_credit_risk.*`) | Curated Layer (`hive.credit_risk.*`) | Transformation & Fallback Logic |
| :--- | :--- | :--- | :--- |
| `raw_application_train`<br>`raw_application_test` | `stage_application_train`<br>`stage_application_test` | `dim_customer` | Clean demographics, deduplicate on `SK_ID_CURR`, calculate `age_years` and `employed_years`. Generate `sk_customer_key = xxhash64(sk_id_curr, valid_from)`. Add `-1` default record. |
| `raw_application_train`<br>`raw_previous_application` | `stage_application_train`<br>`stage_previous_application` | `dim_loan_product` | Extract distinct contract types, map product group and revolving flag. Add `-1` default record. |
| `raw_previous_application` | `stage_previous_application` | `dim_merchant_channel` | Extract distinct combinations of `CHANNEL_TYPE`, `NAME_GOODS_CATEGORY`, and `NAME_SELLER_INDUSTRY`. Add `-1` default record. |
| *Risk Rule Master* | *Risk Code Master* | `dim_delinquency_bucket` | Standardize DPD aging brackets (`B0` to `NPL`). Add `-1` default record. |
| `raw_previous_application` | `stage_previous_application` | `dim_application_decision` | Map contract status outcomes and rejection reasons. Add `-1` default record. |
| *Time Master* | *Relative Time Master* | `dim_relative_time` | Create relative day and month offsets from application date. Add `-1` default record. |
| `raw_bureau` | `stage_bureau` | `dim_bureau_source` | Extract distinct bureau credit types, classify collateral type. Add `-1` default record. |
| `raw_application_train`<br>`raw_previous_application` | `stage_application_train`<br>`stage_previous_application` | `fact_loan_application` | Cast monetary fields to `Decimal(18,2)`. Generate `sk_application_key = xxhash64(...)`. Join dimensions with fallback `coalesce(dim_sk, -1)`. |
| `raw_installments_payments` | `stage_installments_payments` | `fact_installment_payment` | Clean nulls, calculate `payment_delay_days` and `amt_underpayment`. Generate `sk_installment_key = xxhash64(...)`. Fallback `coalesce(dim_sk, -1)`. |
| `raw_pos_cash_balance`<br>`raw_credit_card_balance` | `stage_pos_cash_balance`<br>`stage_credit_card_balance` | `fact_monthly_loan_snapshot` | Calculate revolving credit card utilization ratio, map delinquency buckets. Generate `sk_snapshot_key = xxhash64(...)`. Fallback `coalesce(dim_sk, -1)`. |
| `raw_bureau` | `stage_bureau` | `fact_bureau_credit` | Standardize status, cast debt amounts to `Decimal(18,2)`. Generate `sk_bureau_credit_key = xxhash64(...)`. Fallback `coalesce(dim_sk, -1)`. |
| `raw_bureau_balance` | `stage_bureau_balance` | `fact_monthly_bureau_snapshot` | Map status codes (`C`, `0`..`5`) to delinquency buckets. Generate `sk_bureau_snapshot_key = xxhash64(...)`. Fallback `coalesce(dim_sk, -1)`. |
| `stage_application_train`<br>`stage_previous_application` | `stage_application_train`<br>`stage_previous_application` | `obt_loan_portfolio_360` | Pre-join `fact_loan_application` with conformed dimensions to create a wide table for simple BI reporting. |

---

## 10. Data Quality & Reconciliation Checks

I run simple automated data quality checks between Stage and Curated:

1. **Sum of Amount Check**:
   $$\sum \text{amt\_instalment}_{\text{curated}} = \sum \text{AMT\_INSTALMENT}_{\text{stage}}$$
   The sum of installment amounts in the curated fact table must equal the sum in the stage table.

2. **Foreign Key Completeness**:
   - Every fact row must have a valid foreign key. Missing foreign keys are filled with `-1`, so no fact rows are lost when using `INNER JOIN`.

3. **Dynamic Partition Overwrites**:
   - Spark write jobs use `spark.sql.sources.partitionOverwriteMode=dynamic` so that re-running a job only overwrites the affected partition rather than clearing the entire table.
