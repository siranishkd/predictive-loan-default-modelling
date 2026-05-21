# System Diagram Standards Reference

Based on standard flowchart conventions, here are the required shapes to be used when mapping out our system architecture:

1. **Process (Rectangle `[ ]`)**: Represents any computational step, script, or data transformation.
   * *Example:* `main.py`, `feature_silver_table.py`, "Data Cleaning"
2. **Data / I/O (Parallelogram `[/ /]`)**: Represents raw input data or output data files.
   * *Example:* Raw CSV files, JSON payloads.
3. **Database / Stored Data (Cylinder `[( )]`)**: Represents persistent, structured storage.
   * *Example:* Datamart Parquet tables, Feature Stores, SQL Databases.
4. **Decision (Diamond `{ }`)**: Represents a conditional branch.
   * *Example:* "Is Data Valid?", "Check Anomaly Threshold"
5. **Document (Rectangle with wavy bottom)**: Represents a physical or final report/document.

*Note: In Mermaid.js, we adhere to these by using `[ ]` for python scripts, `[/ /]` for raw CSVs, and `[( )]` for our datamart parquet tables.*
