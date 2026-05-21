import os
import pyspark
import logging
from pyspark.sql import SparkSession

from utils.feature_bronze_table import process_feature_bronze
from utils.feature_silver_table import process_feature_silver
from utils.feature_gold_table import process_feature_gold
# We can also call the Lab 2 scripts if needed, but for ASG 1 we focus on the feature store
# from utils.data_processing_bronze_table import process_bronze_table
# from utils.data_processing_silver_table import process_silver_table
# from utils.data_processing_gold_table import process_labels_gold_table

def main():
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Configure Pipeline Logging with high detail
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting ETL Pipeline for Assignment 1...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ASG1_Medallion_Pipeline") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create datamart directories if they don't exist
    directories = ["datamart/bronze", "datamart/silver", "datamart/gold"]
    for d in directories:
        os.makedirs(d, exist_ok=True)
        logger.info(f"Ensured directory exists: {d}")
        
    logger.info("--- Running Bronze Pipeline ---")
    process_feature_bronze(spark, "datamart/bronze")
    
    logger.info("--- Running Silver Pipeline ---")
    process_feature_silver(spark, "datamart/bronze", "datamart/silver")
    
    logger.info("--- Running Gold Pipeline ---")
    process_feature_gold(spark, "datamart/silver", "datamart/gold")
    
    logger.info("Pipeline completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
