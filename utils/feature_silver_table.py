import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, trim, when, lit

logger = logging.getLogger(__name__)

def check_anomaly_threshold(df, condition, column_name):
    """
    Evaluates how many rows meet the anomaly condition.
    Logs an alert based on the percentage of anomalous rows.
    """
    total_rows = df.count()
    if total_rows == 0:
        return 0
        
    bad_rows = df.filter(condition).count()
    bad_pct = bad_rows / total_rows
    
    msg = f"Data Quality Check ({column_name}): {bad_rows}/{total_rows} ({bad_pct*100:.2f}%) anomalies found."
    
    if bad_pct > 0.20:
        logger.critical(msg + " CRITICAL: Exceeds 20% threshold.")
    elif bad_pct > 0.05:
        logger.warning(msg + " WARNING: Exceeds 5% threshold.")
    elif bad_pct > 0:
        logger.info(msg)
        
    return bad_pct

def process_feature_silver(spark: SparkSession, datamart_bronze_dir: str, datamart_silver_dir: str):
    """
    Clean Bronze data and write to Silver layer.
    """
    logger.info("--- Processing Feature Silver Tables ---")
    
    # 1. Clickstream
    click_path = os.path.join(datamart_bronze_dir, "bronze_feature_clickstream.parquet")
    if os.path.exists(click_path):
        logger.info("Cleaning Clickstream data...")
        df_click = spark.read.parquet(click_path)
        
        # Cast all fe_ columns to double
        for i in range(1, 21):
            col_name = f"fe_{i}"
            df_click = df_click.withColumn(col_name, col(col_name).cast("double"))
        
        df_click = df_click.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        df_click = df_click.dropna(subset=["Customer_ID", "snapshot_date"])
        df_click = df_click.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_clickstream.parquet")
        df_click.write.mode("overwrite").parquet(out_path)
        logger.info(f"Saved silver clickstream to {out_path}")

    # 2. Attributes
    attr_path = os.path.join(datamart_bronze_dir, "bronze_feature_attributes.parquet")
    if os.path.exists(attr_path):
        logger.info("Cleaning Attributes data...")
        df_attr = spark.read.parquet(attr_path)
        
        # Strip string garbage before casting
        df_attr = df_attr.withColumn("Age", regexp_replace(col("Age"), r"[^\d.-]", "").cast("int"))
        
        # Data Quality Alerting & Median Imputation for Age
        age_anomaly_cond = (col("Age") < 18) | (col("Age") > 100) | col("Age").isNull()
        check_anomaly_threshold(df_attr, age_anomaly_cond, "Age")
        
        # Median fill
        median_age = df_attr.approxQuantile("Age", [0.5], 0.01)[0]
        df_attr = df_attr.withColumn("Age", when(age_anomaly_cond, lit(median_age)).otherwise(col("Age")))
        
        # Clean SSN
        df_attr = df_attr.withColumn("SSN", regexp_replace(col("SSN"), r"[^\d-]", ""))
        df_attr = df_attr.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        
        df_attr = df_attr.dropna(subset=["Customer_ID", "snapshot_date"])
        df_attr = df_attr.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_attributes.parquet")
        df_attr.write.mode("overwrite").parquet(out_path)
        logger.info(f"Saved silver attributes to {out_path}")

    # 3. Financials
    fin_path = os.path.join(datamart_bronze_dir, "bronze_feature_financials.parquet")
    if os.path.exists(fin_path):
        logger.info("Cleaning Financials data...")
        df_fin = spark.read.parquet(fin_path)
        
        # Clean numeric columns strictly
        numeric_cols = [
            "Annual_Income", "Monthly_Inhand_Salary", "Num_Bank_Accounts", "Num_Credit_Card", 
            "Interest_Rate", "Num_of_Loan", "Delay_from_due_date", 
            "Num_of_Delayed_Payment", "Changed_Credit_Limit", "Num_Credit_Inquiries",
            "Outstanding_Debt", "Credit_Utilization_Ratio", "Total_EMI_per_month",
            "Amount_invested_monthly", "Monthly_Balance"
        ]
        
        for c in numeric_cols:
            if c in df_fin.columns:
                df_fin = df_fin.withColumn(c, regexp_replace(col(c), r"[^\d.-]", "").cast("double"))
        
        # Data Quality Alerting & Imputation for Num_of_Loan
        loan_anomaly_cond = (col("Num_of_Loan") < 0) | (col("Num_of_Loan") > 50) | col("Num_of_Loan").isNull()
        check_anomaly_threshold(df_fin, loan_anomaly_cond, "Num_of_Loan")
        median_loan = df_fin.approxQuantile("Num_of_Loan", [0.5], 0.01)[0]
        df_fin = df_fin.withColumn("Num_of_Loan", when(loan_anomaly_cond, lit(median_loan)).otherwise(col("Num_of_Loan")))
        
        # Data Quality Alerting & Winsorization (99th percentile) for Annual_Income
        income_anomaly_cond = (col("Annual_Income") < 0) | col("Annual_Income").isNull()
        check_anomaly_threshold(df_fin, income_anomaly_cond, "Annual_Income (Missing/Neg)")
        
        # Cap high incomes at 99th percentile to remove billionaire outliers
        income_p99 = df_fin.approxQuantile("Annual_Income", [0.99], 0.01)[0]
        df_fin = df_fin.withColumn("Annual_Income", 
            when(income_anomaly_cond, lit(0))  # Fill neg/null with 0
            .when(col("Annual_Income") > income_p99, lit(income_p99)) # Cap at 99th percentile
            .otherwise(col("Annual_Income")))
                
        df_fin = df_fin.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        df_fin = df_fin.dropna(subset=["Customer_ID", "snapshot_date"])
        df_fin = df_fin.dropDuplicates(["Customer_ID", "snapshot_date"])
        
        out_path = os.path.join(datamart_silver_dir, "silver_feature_financials.parquet")
        df_fin.write.mode("overwrite").parquet(out_path)
        logger.info(f"Saved silver financials to {out_path}")
        
    # 4. Loans
    loan_path = os.path.join(datamart_bronze_dir, "bronze_feature_loans.parquet")
    if os.path.exists(loan_path):
        logger.info("Cleaning Loans data...")
        df_loans = spark.read.parquet(loan_path)
        
        # Clean numeric columns strictly
        numeric_cols_loans = [
            "tenure", "installment_num", "loan_amt", "due_amt", 
            "paid_amt", "overdue_amt", "balance"
        ]
        
        for c in numeric_cols_loans:
            if c in df_loans.columns:
                df_loans = df_loans.withColumn(c, regexp_replace(col(c), r"[^\d.-]", "").cast("double"))
                
        df_loans = df_loans.withColumn("loan_start_date", to_date(col("loan_start_date"), "yyyy-MM-dd"))
        df_loans = df_loans.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd"))
        
        df_loans = df_loans.dropna(subset=["Customer_ID", "snapshot_date"])
        df_loans = df_loans.dropDuplicates(["Customer_ID", "snapshot_date", "loan_id"])
        
        out_path_loans = os.path.join(datamart_silver_dir, "silver_feature_loans.parquet")
        df_loans.write.mode("overwrite").parquet(out_path_loans)
        logger.info(f"Saved silver loans to {out_path_loans}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("SilverTest").getOrCreate()
    process_feature_silver(spark, "datamart/bronze", "datamart/silver")
