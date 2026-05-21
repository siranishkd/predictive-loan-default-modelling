# Dataset Anomaly Analysis & Handling Strategy

> This document serves as the reference for the PPT slide on datasets.

## Summary of Anomalies Found

| Column | Dataset | Anomaly | Count | % | Handling | Status |
|--------|---------|---------|-------|---|----------|--------|
| `Age` | Attributes | Range: -500 to 8,678 (impossible ages) | 988 | 7.9% | ✅ Median imputation (values outside 18–100) | Implemented |
| `Num_of_Loan` | Financials | Range: -100 to 1,495 (negative & extreme) | 563 | 4.5% | ✅ Median imputation (values outside 0–50) | Implemented |
| `Annual_Income` | Financials | Max = $23.8M (extreme right skew) | ~125 | ~1% | ✅ Winsorized at 99th percentile | Implemented |
| `Monthly_Balance` | Financials | Min = -3.3×10²⁵ (absurd negative) | 1 | <0.01% | 🔲 Cap at 0 or median fill | **Outstanding** |
| `Num_Bank_Accounts` | Financials | Range: -1 to 1,756 (negative & extreme) | 4 neg | <0.1% | 🔲 Cap at [0, 20] | **Outstanding** |
| `Num_Credit_Card` | Financials | Max = 1,499 (extreme) | ~few | <0.1% | 🔲 Cap at 99th percentile | **Outstanding** |
| `Interest_Rate` | Financials | Max = 5,789% (impossible rate) | ~few | <0.1% | 🔲 Cap at [0, 100] | **Outstanding** |
| `Num_of_Delayed_Payment` | Financials | Max = 4,293 (extreme) | ~few | <0.1% | 🔲 Cap at 99th percentile | **Outstanding** |
| `Num_Credit_Inquiries` | Financials | Max = 2,554 (extreme) | ~few | <0.1% | 🔲 Cap at 99th percentile | **Outstanding** |
| `Total_EMI_per_month` | Financials | Max = 81,971 (extreme outlier) | ~few | <0.1% | 🔲 Cap at 99th percentile | **Outstanding** |
| `Changed_Credit_Limit` | Financials | 254 NULLs after type casting | 254 | 2.0% | 🔲 Median fill | **Outstanding** |
| `Credit_Mix` | Financials | Contains garbage value `_` | 2,611 | 20.9% | 🔲 Replace `_` with NULL, then mode fill | **Outstanding** |
| `Payment_Behaviour` | Financials | Contains garbage value `!@9#%8` | 998 | 8.0% | 🔲 Replace with NULL, then mode fill | **Outstanding** |

## Preemptive Safety Checks (Monitoring Backlog)

These are columns where anomalies were NOT found today, but should be monitored in future pipeline runs:

| Column | Dataset | Check | Rationale |
|--------|---------|-------|-----------|
| `Outstanding_Debt` | Financials | Alert if negative values appear | Debt cannot be negative |
| `Credit_Utilization_Ratio` | Financials | Alert if values exceed 100% | Ratio should be 0–100 |
| `Monthly_Inhand_Salary` | Financials | Alert if < 0 or > 50,000 | Salary sanity bounds |
| `paid_amt` | Loans | Alert if negative values appear | Payments cannot be negative |
| `overdue_amt` | Loans | Alert if > `loan_amt` | Cannot owe more than the loan |
| `Occupation` | Attributes | Alert if new categories appear | 16 known categories — schema drift detection |
| `SSN` | Attributes | Alert if format changes | Currently digit-hyphen cleaned |

## How We Handle Each Type

| Strategy | When Used | Example |
|----------|-----------|---------|
| **Median Imputation** | Bounded physical variables with >1% anomalies | Age, Num_of_Loan |
| **Winsorization (Capping)** | Highly skewed unbounded financials | Annual_Income capped at P99 |
| **Mode Fill** | Categorical columns with garbage values | Credit_Mix (`_`), Payment_Behaviour (`!@9#%8`) |
| **Threshold Alerting** | All cleaned columns | <1% INFO, >5% WARNING, >20% CRITICAL |
