from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import subprocess
import tempfile
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv  

# Load environment variables from .env file
load_dotenv()
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY").strip()  

# Define Azure Storage Account & Container
STORAGE_ACCOUNT = "team13adls"
CONTAINER_NAME = "github-realtime-issue"
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/GH_archive_raw"

# Initialize Spark session with Azure ADLS support
spark = SparkSession.builder \
    .appName("LoadGHArchiveData") \
    .config("spark.driver.memory", "8g") \  # 8GB for driver
    .config("spark.executor.memory", "8g") \  # 8GB per executor
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()

# Define start and end timestamps (Loop over 1 year of data)
start_date = datetime(2024, 3, 1, 0)  # Start at 2024-03-01-00
end_date = datetime(2024, 3, 2, 0)    # End at 2025-03-01-00

# Loop over each hourly timestamp
current_date = start_date
while current_date <= end_date:
    # Construct the URL for the given hour
    formatted_date = current_date.strftime("%Y-%m-%d-%-H")  # e.g., 2024-03-01-0
    url = f"https://data.gharchive.org/{formatted_date}.json.gz"
    
    # Create a temp file manually and keep it until Spark reads it
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    temp_file_path = temp_file.name  # Get the temp file path
    temp_file.close()  # Close so subprocess can write to it

    try:
        print(f"Processing {formatted_date}...")

        # Use subprocess to fetch and decompress data into the temp file
        with open(temp_file_path, "wb") as f:
            curl_process = subprocess.Popen(["curl", "-s", url], stdout=subprocess.PIPE)
            gunzip_process = subprocess.Popen(["gunzip", "-c"], stdin=curl_process.stdout, stdout=f)
            gunzip_process.communicate()  # Wait for process to complete

        # Load JSON file into Spark DataFrame
        df = spark.read.json(temp_file_path)

        # Filter and select relevant columns
        df = df.filter(df["type"] == "IssuesEvent").select(
            col("actor"),
            col("created_at"),
            col("id").alias("event_id"),
            col("org"),
            col("payload.action"),
            col("payload.issue")
        )

        # Add partition columns to the DataFrame
        df = df.withColumn("year", col("created_at").substr(1, 4)) \
               .withColumn("month", col("created_at").substr(6, 2)) \
               .withColumn("day", col("created_at").substr(9, 2))

        # Define ADLS save path (Partitioned by year/month/day)
        adls_daily_path = f"{BRONZE_PATH}"

        # Save DataFrame as Parquet to ADLS Gen2 with partitioning
        df.write.mode("append").partitionBy("year", "month", "day").parquet(adls_daily_path)

        print(f"Successfully uploaded {formatted_date} to {adls_daily_path}")

    except Exception as e:
        print(f"Error processing {formatted_date}: {str(e)}")

    finally:
        # Ensure the temp file is deleted even if an error occurs
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

    # Move to the next hour
    current_date += timedelta(hours=1)

print("All hourly data processing completed!")
spark.stop()
