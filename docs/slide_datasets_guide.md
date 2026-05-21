# PPT Slide Guide: Datasets, Anomalies & Quality Control

Use the structured layout below to copy-paste directly into your PowerPoint slide.

---

## Slide Title: Data Engineering Quality Control & Anomaly Mitigation

### Visual Layout Recommendation
*   **Left Column (60% width):** The "Data Quality Scan & Mitigation" table (shows exactly what was found and how it was fixed).
*   **Right Column (40% width):** "Preemptive Health Check Backlog" & "Observability Architecture" (shows what's monitored, warning thresholds, and the logging engine).

---

### [LEFT COLUMN] Data Quality Scan & Mitigation Table

| Dataset | Attribute | Detected Anomaly | System Treatment |
| :--- | :--- | :--- | :--- |
| **Attributes** | `Age` | **7.9% rows** out of bounds (-500 to 8,678) | Bounded cleaning + **Median Imputation** (18-100 range) |
| **Financials** | `Num_of_Loan` | **4.5% rows** negative/extreme (-100 to 1,495) | Bounded cleaning + **Median Imputation** (0-50 range) |
| **Financials** | `Annual_Income` | **Extreme skew** (Max $23.8M; distorts mean) | **Winsorization** (capped at 99th percentile, P99 = $195k) |
| **Financials** | `Credit_Mix` | **20.9% garbage values** (represented as `_`) | *Backlog:* Clean string format, mode-fill NULLs |
| **Financials** | `Payment_Behaviour` | **8.0% garbage values** (`!@9#%8`) | *Backlog:* Clean string format, mode-fill NULLs |
| **Financials** | `Monthly_Balance` | **Extreme negative** (e.g. -3.3e25) | *Backlog:* Impute using customer average |

---

### [RIGHT COLUMN] Preemptive Health Checks & Observability

#### 1. Proactive Pipeline Health Backlog (Future Runs)
*   **Zero-Value Guards:** Alert if financial variables like `Outstanding_Debt` or `overdue_amt` drop below 0.
*   **Range Checks:** Alert if `Credit_Utilization_Ratio` exceeds 100% or `Interest_Rate` exceeds 100%.
*   **Category Guards:** Alert on schema/categorical drift if new occupations or loan types appear.

#### 2. Tiered Quality Alerting (Standardized Log Engine)
*   Our pipeline runs automated metric comparisons and alerts on standard outputs:
    *   **INFO (<1%):** Silent auto-correction (standard imputation).
    *   **WARNING (5%-20%):** Flags dataset drift or minor file corruption.
    *   **CRITICAL (>20%):** Immediate halt or critical warning (severe upstream source corruption).

---

### Speaker Notes (What to say during presentation)
*   *"We ran a full anomaly scan across all four incoming datasets before building the ML feature store. We found that dropping anomalous records would result in a 10% data loss, introducing selection bias."*
*   *"To prevent this, we designed a split strategy: Median Imputation for physical attributes like Age and active loans, and Winsorization for highly skewed financials like Annual Income."*
*   *"Furthermore, we designed a tiered alert system: if anomalies exceed 5%, our Spark execution logs a WARNING; if they exceed 20%, it triggers a CRITICAL alert. Outstanding issues like string garbage in Credit Mix have been cataloged into our backlog for the next sprint."*
