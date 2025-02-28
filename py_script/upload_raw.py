import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import concurrent.futures

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("github_data_ingestion.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY").strip()

# Azure configuration
STORAGE_ACCOUNT = "team13adls"
CONTAINER_NAME = "github-realtime-issue"
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/GH_archive_raw"

# Spark session configuration
spark = SparkSession.builder \
    .appName("LoadGHArchiveData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.1,"
            "com.microsoft.azure:azure-storage:8.6.6,"
            "io.delta:delta-spark_2.12:3.3.0") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

def download_file(url, output_path, max_retries=3):
    """
    Downloads a gzipped JSON file from the given URL and writes it to output_path.
    Retries the download on failure up to max_retries times using exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded {url} to {output_path} on attempt {attempt}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {url} on attempt {attempt}: {e}")
            if attempt < max_retries:
                sleep_time = 2 ** attempt
                logger.info(f"Retrying {url} in {sleep_time} seconds...")
                time.sleep(sleep_time)
    logger.error(f"Failed to download {url} after {max_retries} attempts.")
    return False

# Date range setup
start_date = datetime(2024, 3, 7, 0)
end_date = datetime(2024, 3, 31, 0)  # Adjusted for testing

current_date = start_date
while current_date <= end_date:
    day_str = current_date.strftime("%Y-%m-%d")
    logger.info(f"🚀 Processing day: {day_str}")

    # Create a temporary folder for this day's files
    temp_dir = os.path.join("/tmp", f"gharchive_{day_str}")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Generate URLs and corresponding local file paths for each hour of the day.
        downloads = []
        for h in range(24):
            hour_str = (current_date + timedelta(hours=h)).strftime("%Y-%m-%d-%-H")
            url = f"https://data.gharchive.org/{hour_str}.json.gz"
            local_filename = os.path.join(temp_dir, f"{hour_str}.json.gz")
            downloads.append((url, local_filename))
        
        # Download files concurrently using ThreadPoolExecutor with retries
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(download_file, url, path, 3) for url, path in downloads]
            concurrent.futures.wait(futures)
        
        # Check if all 24 files were successfully downloaded.
        downloaded_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".json.gz")]
        if len(downloaded_files) != 24:
            logger.error(f"Not all 24 files downloaded for {day_str} (only {len(downloaded_files)} downloaded). Skipping processing for this day.")
            continue  # Skip processing and move on to the next day

        # Let Spark read all gzipped JSON files from the temp folder
        df = spark.read.json(os.path.join(temp_dir, "*.json.gz"))
        
        # Filter for IssuesEvent events and transform columns
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
            .withColumn("year", F.year(col("created_date"))) \
            .withColumn("month", F.month(col("created_date"))) \
            .withColumn("day", F.dayofmonth(col("created_date"))) \
            .drop("created_date")
        
        # Write the data to Delta Lake, partitioned by year, month, and day
        (issues_df.write
            .format("delta")
            .mode("append")
            .partitionBy("year", "month", "day")
            .option("optimizeWrite", "true")
            .option("delta.enableChangeDataFeed", "false")
            .save(BRONZE_PATH))
        
        logger.info(f"✅ Successfully processed {len(downloaded_files)} files for {day_str}")
    
    except Exception as e:
        logger.exception(f"❌ Failed to process {day_str}: {e}")
    
    finally:
        # Clear Spark cache and remove the temporary folder for this day
        spark.catalog.clearCache()
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Removed temporary folder {temp_dir}")
        except Exception as del_e:
            logger.error(f"Error deleting temporary folder {temp_dir}: {del_e}")
        current_date += timedelta(days=1)

logger.info("🎉 All daily data processing completed!")
spark.stop()
