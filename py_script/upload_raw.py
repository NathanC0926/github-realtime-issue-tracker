import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Setup logging (unchanged)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("github_data_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables (unchanged)
load_dotenv()
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY").strip()

# Azure configuration (unchanged)
STORAGE_ACCOUNT = "team13adls"
CONTAINER_NAME = "github-realtime-issue"
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/GH_archive_raw"

# Enhanced Spark configuration
spark = SparkSession.builder \
    .appName("LoadGHArchiveData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-azure:3.3.1,"
            "com.microsoft.azure:azure-storage:8.6.6,"
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.https.impl", "org.apache.hadoop.fs.http.HttpsFileSystem") \
    .config("spark.hadoop.fs.http.impl", "org.apache.hadoop.fs.http.HttpFileSystem") \
    .config("spark.hadoop.fs.http.connection.timeout", "60000") \
    .config("spark.hadoop.fs.http.connection.max.per.route", "50") \
    .getOrCreate()

# Date range setup (unchanged)
start_date = datetime(2024, 3, 1, 0)
end_date = datetime(2024, 3, 10, 0)  # Adjusted for testing

current_date = start_date
while current_date <= end_date:
    try:
        day_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"🚀 Processing day: {day_str}")
        
        # Generate all URLs for the day
        urls = [
            f"https://data.gharchive.org/{(current_date + timedelta(hours=h)).strftime('%Y-%m-%d-%-H')}.json.gz"
            for h in range(24)
        ]

        # Bulk read with error handling
        df = spark.read \
            .option("ignoreCorruptFiles", "true") \
            .option("mode", "PERMISSIVE") \
            .json(urls)

        # Filter and transform
        issues_df = df.filter(col("type") == "IssuesEvent") \
            .select(
                col("actor"),
                col("created_at"),
                col("id").alias("event_id"),
                col("org"),
                col("payload.action"),
                col("payload.issue")
            ) \
            .withColumn("created_date", to_date(col("created_at"))) \
            .withColumn("year", year(col("created_date"))) \
            .withColumn("month", month(col("created_date"))) \
            .withColumn("day", dayofmonth(col("created_date"))) \
            .drop("created_date")

        # Optimized write
        (issues_df.write
            .format("delta")
            .mode("append")
            .partitionBy("year", "month", "day")
            .option("optimizeWrite", "true")
            .option("delta.enableChangeDataFeed", "false")
            .save(BRONZE_PATH))

        logger.info(f"✅ Successfully processed {len(urls)} files for {day_str}")

    except Exception as e:
        logger.error(f"❌ Failed to process {day_str}: {str(e)}")
        # Implement retry logic here if needed

    finally:
        # Cleanup Spark cache
        spark.catalog.clearCache()
        current_date += timedelta(days=1)

logger.info("🎉 All daily data processing completed!")
spark.stop()