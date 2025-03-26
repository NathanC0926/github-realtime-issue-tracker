import math
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from transform import process_issues  # Your transformation function

def create_spark_session(total_cores: int,
                         memory_per_executor_gb: int = 8,
                         max_result_size_gb: int = 4,
                         ) -> SparkSession:
    """
    Create and configure a SparkSession for Delta Lake processing.
    
    Args:
        total_cores (int): Total cores available for Spark.
        memory_per_executor_gb (int): Memory per executor in GB.
        max_result_size_gb (int): Maximum result size for the driver in GB.
        
    Returns:
        SparkSession: A configured SparkSession.
    """
    # Compute number of executors as the floor of sqrt(total_cores)
    num_executors = int(math.sqrt(total_cores))
    if num_executors < 1:
        num_executors = 1

    # Divide total cores equally among executors
    cores_per_executor = total_cores // num_executors
    
    print(f"Configuring Spark: {num_executors} executors with {cores_per_executor} cores each.")
    
    # Convert memory settings to string format required by Spark, e.g., "8g"
    memory_str = f"{memory_per_executor_gb}g"
    max_result_str = f"{max_result_size_gb}g"
    
    builder = (SparkSession.builder 
        .appName("MyDeltaApp") 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore") 
        .config("spark.driver.memory", memory_str)
        .config("spark.executor.memory", memory_str)
        .config("spark.executor.cores", str(cores_per_executor))
        .config("spark.executor.instances", str(num_executors))
        .config("spark.driver.maxResultSize", max_result_str)
        .config("spark.sql.adaptive.enabled", "false")
    )
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def load_data(spark: SparkSession, path: str):

    df = spark.read.format("delta").load(path)
    print("✅ DataFrame loaded successfully!")
    return df

def main():
    # Create a Spark session using these parameters.
    spark = create_spark_session(total_cores=8,
                                 memory_per_executor_gb=8,
                                 max_result_size_gb=4)
    
    # Define the path to the Delta table.
    delta_table_path = "file:///projectnb/ds340/projects/nathan_song_ze_chang_Final_Project/data/raw/fct_issue_event"
    raw_df = load_data(spark, delta_table_path)
    
    # Process the DataFrame.
    processed_df = process_issues(raw_df)
    print("✅ DataFrame processed successfully!")
    
    # Show a preview of the processed data.
    processed_df.show(10)
    
    # Stop the Spark session.
    spark.stop()

if __name__ == "__main__":
    main()

