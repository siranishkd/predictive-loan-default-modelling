# Pipeline Architecture v3.0 — Medallion Architecture

> Source of truth: [Databricks — What is Medallion Architecture?](https://www.databricks.com/glossary/medallion-architecture)

## Mermaid Diagram

Copy the code block below and paste it into [mermaid.live](https://mermaid.live) to render.

```mermaid
graph LR
    classDef bronze fill:#cd7f32,stroke:#333,stroke-width:2px,color:#fff
    classDef silver fill:#c0c0c0,stroke:#333,stroke-width:2px,color:#000
    classDef gold fill:#ffd700,stroke:#333,stroke-width:2px,color:#000
    classDef raw fill:#e6e6fa,stroke:#333,stroke-width:1px,color:#000
    classDef process fill:#bbdefb,stroke:#333,stroke-width:2px,color:#000
    classDef log fill:#f5f5f5,stroke:#999,stroke-width:1px,color:#666

    %% Raw Layer
    subgraph Raw
        R1[/Clickstream CSV/]:::raw
        R2[/Attributes CSV/]:::raw
        R3[/Financials CSV/]:::raw
        R4[/Loans CSV/]:::raw
    end

    %% Bronze Layer
    P1[feature_bronze_table.py]:::process

    subgraph Bronze
        B1[(Clickstream)]:::bronze
        B2[(Attributes)]:::bronze
        B3[(Financials)]:::bronze
        B4[(Loans)]:::bronze
    end

    R1 --> P1
    R2 --> P1
    R3 --> P1
    R4 --> P1
    P1 --> B1
    P1 --> B2
    P1 --> B3
    P1 --> B4

    %% Silver Layer
    P2[feature_silver_table.py]:::process

    subgraph Silver
        S1[(Clickstream)]:::silver
        S2[(Attributes)]:::silver
        S3[(Financials)]:::silver
        S4[(Loans)]:::silver
    end

    B1 --> P2
    B2 --> P2
    B3 --> P2
    B4 --> P2
    P2 --> S1
    P2 --> S2
    P2 --> S3
    P2 --> S4

    %% Gold Layer
    P3[feature_gold_table.py]:::process

    subgraph Gold
        G1[(ML Feature Store)]:::gold
    end

    S1 --> P3
    S2 --> P3
    S3 --> P3
    S4 --> P3
    P3 --> G1

    %% Logging — sits below, small
    LOG[/logs/pipeline.log/]:::log
    P1 -.-> LOG
    P2 -.-> LOG
    P3 -.-> LOG
```

## Layer Definitions (per Databricks)

| Layer | Purpose (Databricks) | What Our Code Does (`*.py`) | Our Processing Steps | Anomalies & Logging |
|-------|---------------------|---------------------------|----------------------|---------------------|
| **Bronze** | Land raw data "as-is" with metadata (load date, process ID) | `feature_bronze_table.py` loops through a dictionary of 4 CSV paths. For each file, it reads with `inferSchema=False` (preserves raw strings), adds an `ingestion_timestamp` metadata column, and writes to Parquet. No cleaning, no type casting — raw data preserved exactly as received. | ✅ Read 4 CSVs (Clickstream, Attributes, Financials, Loans) | ✅ `ingestion_timestamp` added to every row |
| | | | ✅ Add `ingestion_timestamp` column | ✅ Row counts logged per table |
| | | | ✅ Write to Parquet (no schema changes) | 🔲 *Backlog: Schema drift detection* |
| **Silver** | Cleanse, conform, deduplicate — "Enterprise view" of key business entities | `feature_silver_table.py` applies "just-enough" transformations per Databricks guidance. It: (1) regex-strips garbage chars from numeric strings, (2) casts columns to correct types (int/double/date), (3) runs `check_anomaly_threshold()` to count and log anomalies before fixing, (4) applies median imputation for bounded vars and winsorization for skewed vars, (5) deduplicates by composite key. | ✅ Cast `Age` to int, regex-strip garbage chars | ✅ **Age**: 988/12500 (7.9%) — `WARNING` |
| | | | ✅ Median-fill Age outliers (outside 18–100) | ✅ **Num_of_Loan**: 563/12500 (4.5%) — `INFO` |
| | | | ✅ Median-fill Num_of_Loan outliers (outside 0–50) | ✅ **Annual_Income**: Winsorized at P99 |
| | | | ✅ Winsorize Annual_Income at 99th percentile | ✅ Threshold alerting: <1% INFO, >5% WARNING, >20% CRITICAL |
| | | | ✅ Regex-clean all financial numeric columns | 🔲 *Backlog: Monthly_Balance has -3.3×10²⁵ outlier* |
| | | | ✅ Cast `loan_start_date`, `snapshot_date` to Date | 🔲 *Backlog: Credit_Mix has 2,611 garbage `_` values (20.9%)* |
| | | | ✅ Deduplicate by `Customer_ID` + `snapshot_date` | 🔲 *Backlog: Payment_Behaviour has 998 `!@9#%8` values (8.0%)* |
| | | | | 🔲 *Backlog: Interest_Rate max=5,789% — cap at 100* |
| **Gold** | Curated, consumption-ready, business-level aggregates and features | `feature_gold_table.py` uses the **Loans** table as the anchor spine (137,500 loan events). For each loan snapshot, it performs ASOF (point-in-time) forward-fill joins to pull the latest available Attributes and Financials without looking into the future. For Clickstream, it computes 90-day rolling `sum` and `avg` windows for all 20 `fe_` features. Finally, it derives `Debt_to_Income` and fills missing clickstream with 0. | ✅ Loans table as anchor spine (137,500 events) | ✅ Gold row count logged |
| | | | ✅ ASOF join: forward-fill Attributes to loan dates | ✅ Zero data leakage guaranteed (point-in-time) |
| | | | ✅ ASOF join: forward-fill Financials to loan dates | 🔲 *Backlog: Alert if Gold rows deviate >10% from Silver Loans* |
| | | | ✅ 90-day rolling window for Clickstream (sum + avg, fe_1–fe_20) | 🔲 *Backlog: Feature drift monitoring across runs* |
| | | | ✅ Derived feature: `Debt_to_Income` ratio | |
| | | | ✅ Fill missing clickstream aggregates with 0.0 | |

## Logging Strategy

We use **one combined pipeline log** (`logs/pipeline.log`) rather than separate logs per layer. This is the standard approach because:
- Logging is **infrastructure**, not a data layer — it sits outside the Medallion hierarchy
- A single chronological log makes it easy to trace the full pipeline execution from Bronze through Gold
- Data Quality threshold alerts (INFO/WARNING/CRITICAL) from Silver and Gold processes both write to this same file
