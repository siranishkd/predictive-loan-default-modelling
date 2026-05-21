# Silver Layer Consolidation Strategy & Key Analysis

This document provides a technical evaluation of whether the Silver tables can be combined, mapping their keys, relationships, and the architectural trade-offs of consolidation.

---

## 1. Primary & Composite Keys by Table

To evaluate consolidation, we first define the grain (primary keys) of each Silver table:

| Silver Table | Cardinality (Rows) | Primary/Composite Key | Relationship Type |
| :--- | :---: | :--- | :--- |
| `silver_feature_attributes` | 12,500 | `(Customer_ID, snapshot_date)` | **1:1** with Financials |
| `silver_feature_financials` | 12,500 | `(Customer_ID, snapshot_date)` | **1:1** with Attributes |
| `silver_feature_loans` | 137,500 | `(Customer_ID, loan_id, snapshot_date)` | **1:N** (One customer has multiple loans) |
| `silver_feature_clickstream` | 215,376 | `(Customer_ID, snapshot_date)` *with duplicates* | **1:N** (Multiple daily clickstream snapshots per month) |

---

## 2. Feasibility of Combination

### A. Attributes & Financials (Highly Consolidatable)
*   **Key Relationship:** 1-to-1 matching on `(Customer_ID, snapshot_date)`.
*   **Feasibility:** 100%. They represent the exact same customer base sampled at the exact same monthly snapshot dates.
*   **Consolidation Strategy:** They can be merged into a single `silver_customer_profile` table using an `INNER JOIN` on `Customer_ID` and `snapshot_date`.

### B. Inbound Clickstream (Cannot be directly joined without aggregation)
*   **Key Relationship:** 1-to-Many. There are multiple clickstream records for the same customer within a month.
*   **Feasibility:** Low. Joining raw clickstream directly to Attributes or Financials would cause row multiplication (Cartesian fan-out), corrupting the demographic and financial values (e.g., doubling their income or age for every clickstream row).
*   **Consolidation Strategy:** Keep separate at Silver. Aggregate using rolling windows (e.g., 90-day activity counts) before joining at Gold.

### C. Loans Table (Our Primary Timeline Spine)
*   **Key Relationship:** 1-to-Many. A customer can hold multiple active loans concurrently.
*   **Feasibility:** Low. Joining demographic/financial profile data to Loans at the Silver layer changes the base unit of observation from *Customer* to *Loan Event*.
*   **Consolidation Strategy:** Keep separate. The Loans table serves as the primary spine. Features should be joined to it on-demand at Gold using Point-in-Time logic.

---

## 3. Architectural Trade-offs of Silver Consolidation

If we consolidate **Attributes** and **Financials** into a single `silver_customer_profile` table:

### Pros (Advantages)
1.  **Reduced Storage Clutter:** Cuts the number of Silver parquet files from 4 to 3, simplifying directory structures.
2.  **Downstream Query Optimization:** Reduces join overhead in PySpark when generating the Gold layer, as the demographics and financials are pre-joined.
3.  **Synchronized DQ Checks:** Easier to run cross-column checks (e.g., verifying if `Annual_Income` aligns with demographic indicators like `Occupation`).

### Cons (Disadvantages)
1.  **Violation of Domain Separation:**
    *   *Attributes* contains PII and CRM data (Name, SSN, Age, Occupation).
    *   *Financials* contains credit history and debt values.
    *   Keeping them separate allows different access control policies (e.g., restricting PII access using row/column level masking). Combining them merges data ownership boundaries.
2.  **Pipeline Tight Coupling:** If the source system for demographic attributes fails, a consolidated Silver table pipeline might fail entirely. Keeping them separate guarantees that financial cleaning can proceed independently.

---

## 4. Final Recommendation for Slide / Report
For a production-grade ML architecture, **keep the Silver tables separate** to respect data ownership boundaries and access permissions (PII masking). 

Instead, perform the consolidation downstream in the **Gold Process** (`feature_gold_table.py`), where all features are combined into a de-normalized, single-table view optimized for model consumption.
