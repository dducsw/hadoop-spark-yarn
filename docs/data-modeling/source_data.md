# Home Credit Default Risk: Source Data Architecture

## 1. Overview & Business Context

The **Home Credit Default Risk** dataset represents a real-world enterprise lending and financial risk environment. Home Credit strives to broaden financial inclusion for the unbanked population by providing a positive and safe borrowing experience.

In financial technology (Fintech) and banking, credit risk scoring models predict whether an applicant will have payment difficulties on a loan. The data platform ingests raw transactional, credit bureau, installment, and application data to produce reliable feature stores and credit scoring marts.

---

## 2. Entity Relationship Diagram (ERD)

<p align="center">
  <img src="../images/source_data.png" alt="Home Credit Source Data ERD" width="850" />
</p>

### Relationship Matrix & Cardinality

| Primary Entity / Table | Child / Related Table | Join Key | Cardinality | Description |
| :--- | :--- | :--- | :--- | :--- |
| `application_train` / `test` | `bureau` | `SK_ID_CURR` | $1 : N$ | Credit history with other financial institutions reported to Credit Bureau. |
| `bureau` | `bureau_balance` | `SK_ID_BUREAU` | $1 : N$ | Monthly status and historical balance snapshots in Credit Bureau. |
| `application_train` / `test` | `previous_application` | `SK_ID_CURR` | $1 : N$ | Past internal loan applications submitted to Home Credit. |
| `previous_application` | `POS_CASH_balance` | `SK_ID_PREV` (`SK_ID_CURR`) | $1 : N$ | Monthly balance snapshots for point-of-sale and cash loans. |
| `previous_application` | `credit_card_balance` | `SK_ID_PREV` (`SK_ID_CURR`) | $1 : N$ | Monthly credit card statements, balance, limits, and draws. |
| `previous_application` | `installments_payments` | `SK_ID_PREV` (`SK_ID_CURR`) | $1 : N$ | Granular repayment history (scheduled vs. actual payment dates & amounts). |

---

## 3. Dataset Breakdown & Schema Reference

### 1. `application_train` / `application_test`
- **Granularity**: 1 row per loan application at Home Credit.
- **Primary Key**: `SK_ID_CURR`
- **Target Variable**: `TARGET` (`1` = Client had payment difficulties / default, `0` = All other cases). *Only present in `application_train`*.
- **Key Feature Domains**:
  - **Loan Characteristics**: `NAME_CONTRACT_TYPE`, `AMT_CREDIT`, `AMT_ANNUITY`, `AMT_GOODS_PRICE`.
  - **Client Demographics & Financials**: `AMT_INCOME_TOTAL`, `NAME_INCOME_TYPE`, `NAME_EDUCATION_TYPE`, `DAYS_BIRTH`, `DAYS_EMPLOYED`.
  - **External Bureau Scores**: `EXT_SOURCE_1`, `EXT_SOURCE_2`, `EXT_SOURCE_3` (Normalized risk scores from external credit bureaus).
  - **Social / Housing Indicators**: Family size, building/housing attributes, document verification flags.

### 2. `bureau`
- **Granularity**: 1 row per credit line opened by the client at other financial institutions (reported to Credit Bureau).
- **Primary Key**: `SK_ID_BUREAU`
- **Foreign Key**: `SK_ID_CURR`
- **Key Feature Domains**:
  - `CREDIT_ACTIVE`: Status of the external credit (Closed / Active / Sold).
  - `DAYS_CREDIT`: How many days before current application did the client apply for Bureau credit.
  - `AMT_CREDIT_SUM`, `AMT_CREDIT_SUM_DEBT`, `AMT_CREDIT_SUM_OVERDUE`, `AMT_CREDIT_SUM_LIMIT`: Balance, debt, and limit metrics across external lenders.
  - `CNT_CREDIT_PROLONG`: Number of times the credit was extended.

### 3. `bureau_balance`
- **Granularity**: 1 row per month of history for each credit line in `bureau`.
- **Primary Key**: Composite (`SK_ID_BUREAU`, `MONTHS_BALANCE`)
- **Foreign Key**: `SK_ID_BUREAU`
- **Key Feature Domains**:
  - `MONTHS_BALANCE`: Month of extracted data relative to application date (`0` = current month, `-1` = 1 month ago, `-2` = 2 months ago...).
  - `STATUS`: Status of credit with Credit Bureau during the month (`C` = closed, `0` = no DPD/overdue, `1` = 1-30 DPD, `2` = 31-60 DPD, ..., `5` = 120+ DPD).

### 4. `previous_application`
- **Granularity**: 1 row per previous loan application submitted to Home Credit.
- **Primary Key**: `SK_ID_PREV`
- **Foreign Key**: `SK_ID_CURR`
- **Key Feature Domains**:
  - `NAME_CONTRACT_STATUS`: Approval outcome (`Approved`, `Canceled`, `Refused`, `Unused offer`).
  - `AMT_APPLICATION` vs `AMT_CREDIT`: Applied amount vs approved credit amount.
  - `RATE_DOWN_PAYMENT`, `RATE_INTEREST_PRIMARY`: Financing rates and downpayment metrics.
  - `DAYS_DECISION`: Relative timestamp when previous application decision was made.

### 5. `POS_CASH_balance`
- **Granularity**: 1 row per month of history for each active/closed POS or cash loan contract.
- **Primary Key**: Composite (`SK_ID_PREV`, `MONTHS_BALANCE`)
- **Foreign Keys**: `SK_ID_PREV`, `SK_ID_CURR`
- **Key Feature Domains**:
  - `CNT_INSTALMENT`, `CNT_INSTALMENT_FUTURE`: Term duration and remaining installments.
  - `NAME_CONTRACT_STATUS`: Monthly loan status (`Active`, `Completed`, `Signed`, `Returned to the store`).
  - `SK_DPD`, `SK_DPD_DEF`: Days Past Due (DPD) and DPD with tolerance for small amounts.

### 6. `credit_card_balance`
- **Granularity**: 1 row per month of history for each credit card contract with Home Credit.
- **Primary Key**: Composite (`SK_ID_PREV`, `MONTHS_BALANCE`)
- **Foreign Keys**: `SK_ID_PREV`, `SK_ID_CURR`
- **Key Feature Domains**:
  - `AMT_BALANCE`, `AMT_CREDIT_LIMIT_ACTUAL`: Utilization metrics.
  - `AMT_DRAWINGS_ATM_CURRENT`, `AMT_DRAWINGS_POS_CURRENT`: Cash vs Merchant spending behavior.
  - `AMT_PAYMENT_TOTAL_CURRENT`: Monthly payment amount made by the cardholder.
  - `SK_DPD`, `SK_DPD_DEF`: Delinquency flags on revolving credit.

### 7. `installments_payments`
- **Granularity**: 1 row per payment installment made (or missed) on past Home Credit loans.
- **Primary Key**: Composite (`SK_ID_PREV`, `NUM_INSTALMENT_NUMBER`, `NUM_INSTALMENT_VERSION`)
- **Foreign Keys**: `SK_ID_PREV`, `SK_ID_CURR`
- **Key Feature Domains**:
  - `NUM_INSTALMENT_VERSION`: Schedule version (`0` indicates credit card or revolving installment).
  - `DAYS_INSTALMENT` vs `DAYS_ENTRY_PAYMENT`: Due date vs actual payment date (key indicator for early vs late repayment velocity).
  - `AMT_INSTALMENT` vs `AMT_PAYMENT`: Scheduled installment amount vs actual amount paid (indicates partial payments or underpayment).

---

## 4. Business Goals: Business Intelligence (BI) & Machine Learning (ML)

The data platform transforms raw disparate transactional records into optimized data layers specifically engineered to power two downstream consumer engines: **Business Intelligence (OLAP Dashboards)** and **Machine Learning (Predictive Modeling)**.

### A. Business Intelligence (BI) & Analytical Goals

1. **Executive Portfolio Risk & Health Dashboards**:
   - **Vintage Analysis**: Track loan cohort default curves across disbursement vintages over $6, 12, 24$ months.
   - **Delinquency Roll Rate Matrix**: Monitor transition rates between delinquency buckets ($\text{Current} \rightarrow \text{30 DPD} \rightarrow \text{60 DPD} \rightarrow \text{90+ DPD}$).
   - **Portfolio NPL Tracking**: Real-time aggregation of total active debt, overdue amounts, and bad debt ratios served via ClickHouse OLAP.

2. **Loan Origination & Underwriting Funnel Analytics**:
   - Monitor application throughput, approval/rejection rates, and loan officer/system turnaround times (TAT) across contract types (`Cash loans`, `Revolving loans`).

3. **Customer Segmentation & Risk Profiling**:
   - Multidimensional slices by client demographics (income brackets, education level, occupation type, family status) against credit performance.

4. **Merchant & POS Channel Performance**:
   - Analyze disbursement volume, average loan ticket size, and merchant-specific default rates across Point-of-Sale (POS) retail partners.

---

### B. Machine Learning (ML) Goals & Use Cases

1. **Credit Default Probability Prediction (Binary Classification)**:
   - **Objective**: Train supervised ML models (LightGBM, XGBoost, CatBoost, Neural Networks) to accurately predict loan default probability (`P(TARGET = 1)`).
   - **Business Impact**: Reduce Non-Performing Loans (NPLs), optimize credit loss provisioning, and automate instant loan approval decisions.

2. **Automated Feature Store & Feature Engineering**:
   - **Aggregated Historical Signals**: Aggregate $1:N$ behavioral footprints from child tables (`bureau`, `POS_CASH_balance`, `credit_card_balance`, `installments_payments`) into customer-level features (e.g. max DPD, average payment ratio, credit utilization velocity).
   - **Leakage Prevention**: Ensure strict point-in-time cutoff consistency relative to loan application date (`DAYS_DECISION`, `MONTHS_BALANCE`).

3. **Credit Risk Scorecards & Cut-off Policy**:
   - Convert continuous probability predictions into calibrated credit scorecards (e.g., $300 - 850$ scale) to define clear approval, manual review, and rejection cut-off tiers.

4. **Delinquency Early Warning System (EWS)**:
   - Leverage monthly longitudinal balance snapshots to detect deterioration in repayment velocity or sudden credit card utilization spikes prior to formal default.

---

## 5. Key Engineering Considerations & Challenges

1. **Temporal Features & Negative Relative Days**:
   - Timestamps in the dataset are normalized as negative integer offsets (e.g., `DAYS_BIRTH = -15000` means 15,000 days before the current application).
   - Monthly balance files use `MONTHS_BALANCE` ($0, -1, -2, \dots$) requiring window aggregations (e.g. 3-month vs 12-month rolling trends).

2. **One-to-Many Aggregation Strategy**:
   - Tables such as `bureau` and `installments_payments` have multiple rows per client (`SK_ID_CURR`).
   - Downstream curated feature jobs must aggregate these using statistical summaries (`MIN`, `MAX`, `AVG`, `SUM`, `STDDEV`) grouped by `SK_ID_CURR` before joining with `application_train/test`.

3. **High Data Skew & Join Optimization**:
   - Installments and credit card balance tables contain millions of records. Joining these with `application_train` on `SK_ID_CURR` requires partition pruning, broadcast joins for small lookup tables, and dynamic shuffle partition sizing on YARN.

4. **Data Integrity & Reconciliation**:
   - In financial data pipelines, record counts and monetary totals (`AMT_CREDIT`, `AMT_PAYMENT`) must be strictly audited and reconciled between Raw $\rightarrow$ Stage $\rightarrow$ Curated layers.
