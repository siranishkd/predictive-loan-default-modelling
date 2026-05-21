import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, last as _last
from pyspark.sql.window import Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def process_feature_gold(spark: SparkSession, datamart_silver_dir: str, datamart_gold_dir: str):
    """
    Construct the Gold Feature Store.
    Performs Point-in-Time aggregations and ASOF joins to avoid Data Leakage.
    """
    logger.info("--- Processing Feature Gold Tables (v3.0 ASOF with Loans Spine) ---")
    
    silver_click_path = os.path.join(datamart_silver_dir, "silver_feature_clickstream.parquet")
    silver_profile_path = os.path.join(datamart_silver_dir, "silver_customer_profile.parquet")
    silver_loan_path = os.path.join(datamart_silver_dir, "silver_feature_loans.parquet")
    
    if not (os.path.exists(silver_click_path) and os.path.exists(silver_profile_path) and os.path.exists(silver_loan_path)):
        logger.error("Silver tables not ready. Skipping Gold.")
        return
        
    df_click = spark.read.parquet(silver_click_path).drop("ingestion_timestamp")
    df_profile = spark.read.parquet(silver_profile_path).drop("ingestion_timestamp")
    df_loans = spark.read.parquet(silver_loan_path).drop("ingestion_timestamp")
    
    # The Loans table is our universe of target events. We anchor everything to its snapshot_date
    df_base = df_loans.select("Customer_ID", "snapshot_date").distinct()
    
    # Helper for forward filling
    w_asof = Window.partitionBy("Customer_ID").orderBy("snapshot_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # ---------------------------------------------------------
    # 1. ASOF Join for Customer Profile (Merged Demographics & Financials)
    # ---------------------------------------------------------
    df_timeline = df_base.select("Customer_ID", "snapshot_date") \
        .unionByName(df_profile.select("Customer_ID", "snapshot_date")) \
        .distinct()
        
    df_timeline_all = df_timeline \
        .join(df_profile, ["Customer_ID", "snapshot_date"], "left")
        
    cols_to_fill = [c for c in df_timeline_all.columns if c not in ["Customer_ID", "snapshot_date"]]
    for c in cols_to_fill:
        df_timeline_all = df_timeline_all.withColumn(c, _last(col(c), ignorenulls=True).over(w_asof))
        
    df_feat = df_loans.join(df_timeline_all, ["Customer_ID", "snapshot_date"], "inner")
    
    if "Outstanding_Debt" in df_feat.columns and "Annual_Income" in df_feat.columns:
        df_feat = df_feat.withColumn("Debt_to_Income", col("Outstanding_Debt") / (col("Annual_Income") + 1))
        
    # ---------------------------------------------------------
    # 2. Dynamic Clickstream Aggregations (Time-windowed)
    # ---------------------------------------------------------
    df_click_ts = df_click.withColumn("ts", F.unix_timestamp("snapshot_date"))
    days_90 = 90 * 86400
    w_click = Window.partitionBy("Customer_ID").orderBy("ts").rangeBetween(-days_90, 0)
    
    select_exprs = [col("Customer_ID"), col("snapshot_date"), col("ts")]
    for i in range(1, 21):
        select_exprs.append(col(f"fe_{i}"))
        
    df_click_agg = df_click_ts.select(*select_exprs)
    
    for i in range(1, 21):
        col_name = f"fe_{i}"
        df_click_agg = df_click_agg \
            .withColumn(f"{col_name}_sum_90d", _sum(col_name).over(w_click)) \
            .withColumn(f"{col_name}_avg_90d", _avg(col_name).over(w_click))
            
    df_click_agg = df_click_agg.drop("ts", *[f"fe_{i}" for i in range(1, 21)])
    df_click_agg = df_click_agg.dropDuplicates(["Customer_ID", "snapshot_date"])
    
    df_timeline_click = df_timeline.join(df_click_agg, ["Customer_ID", "snapshot_date"], "left")
    
    agg_cols = [c for c in df_click_agg.columns if c not in ["Customer_ID", "snapshot_date"]]
    for c in agg_cols:
        df_timeline_click = df_timeline_click.withColumn(c, _last(col(c), ignorenulls=True).over(w_asof))
        
    df_gold = df_feat.join(df_timeline_click, ["Customer_ID", "snapshot_date"], "inner")
    
    fill_dict = {c: 0.0 for c in agg_cols}
    df_gold = df_gold.fillna(fill_dict)
    
    out_path = os.path.join(datamart_gold_dir, "gold_feature_store.parquet")
    df_gold.write.mode("overwrite").parquet(out_path)
    logger.info(f"Saved gold feature store to {out_path} with {df_gold.count()} rows.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark = SparkSession.builder.appName("GoldTest").getOrCreate()
    process_feature_gold(spark, "datamart/silver", "datamart/gold")
