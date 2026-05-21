# Presentation Deck Content Plan (Max 10 Slides)

This document contains the slide-by-slide copy, layout designs, and speaker notes for your 10-slide presentation deck. 

---

### Slide 1: Title Slide
*   **Slide Title:** Production-Grade Medallion Data Pipeline for Predictive Loan Default Modelling
*   **Subtitle:** Building a Leakage-Free ML Feature Store with Automated Data Quality & Observability
*   **Visual Layout:** Sleek dark background with corporate styling, title in large premium font.
*   **Key Content:**
    *   **Author:** Anish
    *   **Date:** 21 May 2026
    *   **GitHub Repository:** [predictive-loan-default-modelling](https://github.com/siranishkd/predictive-loan-default-modelling)
*   **Speaker Notes:**
    *   *"Good day. Today I am presenting the data engineering pipeline designed to ingest, clean, and engineer features for our predictive loan default model. The architecture is fully containerized, compliant with Databricks Medallion standards, and designed specifically to prevent any temporal data leakage."*

---

### Slide 2: Business Context & Objective
*   **Slide Title:** Business Context & Machine Learning Objective
*   **Visual Layout:** Two-column split layout. Left side focuses on the business problem; right side defines the ML goal.
*   **Key Content (Left):**
    *   **The Business Problem:** Unmanaged loan defaults directly impact credit reserves and profitability. Early identification of high-risk customers allows for targeted collection strategies.
    *   **Data Silos:** Raw customer data resides across disconnected systems: demographic logs, credit records, transaction clickstreams, and active loan books.
*   **Key Content (Right):**
    *   **ML Objective:** Build a clean, time-series matched Feature Store and Label Store suitable for a binary classification model (predicting default within a specified Month-on-Book window).
    *   **Core Engineering Mandate:** Guarantee **zero temporal data leakage** while automating data quality cleaning.
*   **Speaker Notes:**
    *   *"To build a model that predicts default, we must first build the feature store. The challenge is that customer financials and clickstream data change over time. If we join data from the future into our training set, the model will fail in production. Our pipeline resolves this."*

---

### Slide 3: Medallion Architecture Diagram (v3.0)
*   **Slide Title:** Medallion Pipeline Architecture & Flow
*   **Visual Layout:** Horizontal (Left-to-Right) flowchart showing:
    *   **Raw Layer (Parallelograms):** Clickstream, Attributes, Financials, and Loans CSVs.
    *   **Bronze Layer (Cylinders):** As-is Parquet storage with ingestion metadata.
    *   **Silver Layer (Cylinders):** Cleaned, conformed Customer Profile, Clickstream, and Loans Parquet.
    *   **Gold Layer (Cylinders):** Final Curated ML Feature Store and ML Label Store.
    *   **Processes (Rectangles):** Ingestion, Cleaning, and Aggregation scripts.
    *   **Infrastructure (Bottom):** Centralized `pipeline.log` file tracking all layers.
*   **Key Content:** Copy the Mermaid diagram from `docs/architecture_v3.md` directly into this slide.
*   **Speaker Notes:**
    *   *"This is our Medallion pipeline. It runs horizontally. Raw CSVs are ingested into Bronze exactly as-is. In Silver, we clean the data and merge demographics and financials into a single profile. In Gold, we combine everything using ASOF joins."*

---

### Slide 4: Raw Ingestion & Bronze Layer (Archival)
*   **Slide Title:** Bronze Layer: Raw Archival & Schema Preservation
*   **Visual Layout:** Split screen. Left shows ingestion properties; right shows metadata structure.
*   **Key Content:**
    *   **Schema Isolation:** Read using `inferSchema=False` to preserve raw strings, preventing PySpark from guessing types and corrupting invalid characters.
    *   **Ingestion Metadata:** Every table is appended with a `ingestion_timestamp` column to track when the file was processed.
    *   **Parquet Format:** Data is saved in compressed Parquet format, ensuring fast queries and reduced disk space.
    *   **Ingested Volumes:**
        *   Clickstream: 215,376 rows
        *   Loans: 137,500 rows
        *   Attributes: 12,500 rows
        *   Financials: 12,500 rows
*   **Speaker Notes:**
    *   *"In the Bronze layer, our primary goal is schema preservation. We do not clean the data here. We load it as strings, add an ingestion timestamp, and write to Parquet. This provides a raw archival copy that we can always re-process if cleaning rules change."*

---

### Slide 5: Exploratory Data Analysis & Anomalies
*   **Slide Title:** Exploratory Data Analysis: Key Anomalies Discovered
*   **Visual Layout:** A structured summary table highlighting data corruption.
*   **Key Content:**
    *   **Age:** 7.9% of records contain impossible values (e.g. negative ages or ages up to 8,678 years).
    *   **Num_of_Loan:** 4.5% of records contain negative or extreme loan counts (-100 to 1,495).
    *   **Annual_Income:** Extreme right-skewed values (max $23.8M) which heavily distort model regression weights.
    *   **Categorical Garbage:** `Credit_Mix` contains `_` values (20.9%), and `Payment_Behaviour` contains random symbols `!@9#%8` (8.0%).
*   **Speaker Notes:**
    *   *"During EDA, we found significant data corruption. Dropping these anomalous rows would lead to a 10% loss of our dataset, which introduces selection bias. Therefore, we developed a cleaning strategy to impute or winsorize these values."*

---

### Slide 6: Silver Layer: Cleaning & Profile Consolidation
*   **Slide Title:** Silver Layer: Cleaning & Customer Profile Consolidation
*   **Visual Layout:** Left box: Imputation Strategies; Right box: Consolidation Rationale.
*   **Key Content:**
    *   **Imputation & Capping:**
        *   `Age` & `Num_of_Loan`: anomalous values filled with the dataset median.
        *   `Annual_Income`: winsorized (capped) at the 99th percentile ($195,000) to eliminate extreme outliers.
    *   **Consolidation Decision:**
        *   Cleaned Attributes and Financials both share a 1-to-1 relationship on `(Customer_ID, snapshot_date)` and contain exactly 12,500 rows.
        *   We merge them into `silver_customer_profile.parquet` via an `INNER JOIN`.
        *   *Benefit:* Reduces storage clutter (from 4 files to 3) and optimizes downstream Spark joins.
*   **Speaker Notes:**
    *   *"In the Silver layer, we clean the columns and consolidate Attributes and Financials. Since they share the same primary keys and represent the same monthly observations, joining them into a single Customer Profile simplifies our directory structure."*

---

### Slide 7: Gold Layer: The Loans Temporal Spine
*   **Slide Title:** Gold Layer: Establishing the Temporal Spine
*   **Visual Layout:** High-contrast box highlighting the timeline anchor.
*   **Key Content:**
    *   **What is the Spine?** The daily Loans table (`silver_feature_loans`) contains 137,500 active loan records over time.
    *   **Why is it the Spine?** A customer's risk state changes at different loan snapshots. The Loans table provides the exact `snapshot_date` for every observation.
    *   **Preventing Data Leakage:**
        *   We anchor all feature joins to the Loans `snapshot_date`.
        *   Features are only joined if they were recorded **on or before** the loan snapshot date.
        *   This guarantees that no future information (e.g. payments made next month) leaks into the model.
*   **Speaker Notes:**
    *   *"The most critical design decision in this pipeline is the Gold spine. We use the Loans table as our timeline anchor. Every feature we join to a loan event must exist before or on the loan's snapshot date, ensuring zero future leakage."*

---

### Slide 8: Gold Layer: Point-in-Time & Temporal Split
*   **Slide Title:** Gold Layer: Point-in-Time & Temporal Splits
*   **Visual Layout:** Two-column layout. Left: ASOF logic & Splits; Right: Clickstream Windowing.
*   **Key Content (Left):**
    *   **ASOF Join Logic:** Generate timeline, left-join customer profile, and forward-fill values up to loan snapshot using window functions.
    *   **Temporal Split Column:** Added `dataset_split` directly into the Gold table schema:
        *   `snapshot_date < '2025-05-01'` labeled as `Train` (123,394 rows).
        *   `snapshot_date >= '2025-05-01'` labeled as `OOT` (14,106 rows).
        *   *Benefit:* Restricts testing to future snapshots, preventing temporal leakage during model evaluation.
    *   **Derived Feature:** Engineered `Debt_to_Income` dynamically using point-in-time metrics.
*   **Key Content (Right):**
    *   **Time-Windowed Clickstream:**
        *   Clickstream features change rapidly.
        *   We compute a rolling **90-day window** for clickstream columns `fe_1` to `fe_20`.
        *   Calculates rolling `sum` and `avg` for each customer prior to the loan date.
*   **Speaker Notes:**
    *   *"To connect customer profile and clickstream history, we use ASOF joins. To guarantee proper validation, we also engineered a temporal split column in our Gold schema: labeling events before May 2025 as Train and events after as Out-of-Time (OOT). This ensures the ML model is tested on clean future data."*


---

### Slide 9: Observability: Pipeline Logging & Alerting
*   **Slide Title:** Observability: Centralized Pipeline Logging & Alerts
*   **Visual Layout:** Clean layout showing an example log snippet and alert thresholds.
*   **Key Content:**
    *   **Centralized Infrastructure:** A single log file `logs/pipeline.log` records the chronological flow of all pipeline layers.
    *   **Tiered Quality Alerting:**
        *   **INFO (<1% anomalies):** Normal execution, silent auto-correction.
        *   **WARNING (5% - 20% anomalies):** Flags major dataset drift (e.g., Age anomaly at 7.9%).
        *   **CRITICAL (>20% anomalies):** Indicates severe upstream source corruption.
*   **Log Output Snippet:**
    ```text
    INFO - [main.py:25] - Starting ETL Pipeline...
    WARNING - [feature_silver_table.py:25] - DQ Check (Age): 988/12500 (7.90%) anomalies found. WARNING: Exceeds 5% threshold.
    INFO - [feature_gold_table.py:94] - Saved gold feature store with 137500 rows.
    ```
*   **Speaker Notes:**
    *   *"For observability, we implemented standard logging with tiered anomaly thresholds. If a column's anomaly rate exceeds 5%, a WARNING is logged; if it exceeds 20%, it triggers a CRITICAL status. This ensures data engineering teams are immediately aware of data quality drops."*

---

### Slide 10: Model Validation & Backlog Roadmap
*   **Slide Title:** Model Validation & Production Backlog
*   **Visual Layout:** Two columns. Left: Leakage validation results; Right: Backlog roadmap.
*   **Key Content (Left):**
    *   **Sanity Check:** Trained a PySpark Logistic Regression model using our Gold Feature Store and Gold Label Store.
    *   **Result:** The model trained successfully and executed without data shape or leakage errors, confirming it is ready for Task 2's classification task.
*   **Key Content (Right):**
    *   **Outstanding Backlog:**
        *   Impute `Monthly_Balance` negative values (e.g., -3.3e25).
        *   Clean string formatting and mode-fill `Credit_Mix` and `Payment_Behaviour`.
        *   Implement automated schema drift detection at the Bronze layer.
*   **Speaker Notes:**
    *   *"As a final sanity check, we ran a simple Logistic Regression model. It completed training successfully without leakage errors. The outstanding issues, such as cleaning categorical garbage values, have been added to our roadmap backlog for the next iteration. Thank you."*
