# Data Cleaning Strategy (Bronze to Silver)

## 1. Context & EDA Findings
During our Exploratory Data Analysis, we identified severe anomalies in the raw data that require handling before feature engineering in the Gold layer.

### Anomalies Detected:
- **Age (Attributes):** Ranged from -500 to 8,678 years. Approximately 7.9% of the dataset had impossible ages (outside the 18-100 range).
- **Num_of_Loan (Financials):** Ranged from -100 to 1,495. Approximately 4.5% of the dataset had impossible loan counts (outside the 0-50 range).
- **Annual_Income (Financials):** Highly right-skewed with a maximum of nearly $24 Million, distorting the mean heavily.

## 2. Architectural Decisions
Because dropping the anomalous rows entirely would result in a data loss of almost ~10%, which could severely bias our downstream machine learning models, we have chosen the following strategies:

### A. Median Imputation
For bounded physical variables like `Age` and `Num_of_Loan`, we will compute the median of the valid data and fill the anomalous values with this median. This preserves the customer's other valid metrics (like their credit history) without corrupting the dataset.

### B. Winsorization (Capping)
For highly skewed financial variables like `Annual_Income`, we will cap the values at the 99th percentile to prevent billionaires from distorting standard regression coefficients.

## 3. Data Quality Alerting
We have implemented a Data Quality validation threshold in the Silver layer using Python's `logging` module.

- **< 1% Anomalies:** Logs as `INFO` (Silent auto-correction).
- **> 5% Anomalies:** Logs as `WARNING` (Data drift warning).
- **> 20% Anomalies:** Logs as `CRITICAL` (System alerting for heavily corrupted raw data).
