from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Flag to control Azure upload
upload_to_azure = False # Set to True to upload to Azure

# Start Spark session
spark = (
    SparkSession.builder.appName("FilterGHJsonPYTHON")
    .getOrCreate()
)

# Read JSON files from the directory
df = spark.read.json("datap/*.json.gz")

# Filter for IssuesEvent
df = df.filter(df["type"] == "IssuesEvent")

df = df.select(
    col("actor"),
    col("created_at"),
    col("id").alias("event_id"),
    col("org"),
    col("payload.action"),
    col("payload.issue")
)

if upload_to_azure:
    try:
        from azure.storage.filedatalake import DataLakeServiceClient

        # Azure credentials
        account_name = "your_storage_account_name"
        account_key = "your_storage_account_key"
        container_name = "your_container_name"
        directory_name = "your_directory_name"
        parquet_filename = "filtered_issues_events.parquet"

        # Authenticate and create a client
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )

        # Get the file system client
        file_system_client = service_client.get_file_system_client(container_name)

        # Upload directly from Spark DataFrame to Azure Data Lake
        azure_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{directory_name}/{parquet_filename}"
        
        df.write.mode("overwrite").parquet(azure_path)

        print("Parquet file successfully uploaded to Azure Data Lake Gen2.")

    except Exception as e:
        print(f"Azure upload failed: {e}")

else:
    # Save locally
    parquet_file = "./filtered_issues_events.parquet"
    df.write.mode("overwrite").parquet(parquet_file)
    print(f"Parquet file saved locally: {parquet_file}")

# Stop Spark session
spark.stop()
