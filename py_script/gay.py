from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import subprocess
import tempfile
import os
from datetime import datetime, timedelta

# Define Azure Storage Account & Container
STORAGE_ACCOUNT = "your_storage_account"
CONTAINER_NAME = "your_container"
AZURE_ADLS_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/github/issues_events"

# Initialize Spark session with Azure ADLS support
spark = SparkSession.builder \
    .appName("LoadGHArchiveData") \
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", "your_account_key") \
    .getOrCreate()

# Define start and end timestamps (Loop over 1 year of data)
start_date = datetime(2024, 3, 1, 0)  # Start at 2024-03-01-00
end_date = datetime(2025, 3, 1, 0)    # End at 2025-03-01-00

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

        # Define hourly ADLS path (Each hour saved separately)
        adls_hourly_path = f"{AZURE_ADLS_PATH}/year={current_date.year}/month={current_date.month}/day={current_date.day}/hour={current_date.hour}"

        # Save DataFrame as Parquet to ADLS Gen2
        df.write.mode("overwrite").parquet(adls_hourly_path)

        print(f"Successfully uploaded {formatted_date} to {adls_hourly_path}")

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

