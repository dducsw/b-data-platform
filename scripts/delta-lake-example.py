from pyspark.sql import SparkSession
from delta import *

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Sample data
        data = [
            (1, "Alice", 25, "Engineer"),
            (2, "Bob", 30, "Manager"),
            (3, "Charlie", 35, "Analyst"),
            (4, "Diana", 28, "Designer")
        ]
        
        columns = ["id", "name", "age", "role"]
        df = spark.createDataFrame(data, columns)
        
        # Write to Delta table in MinIO
        delta_path = "s3a://datalake/delta-tables/employees"
        
        print("Writing data to Delta table...")
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .save(delta_path)
        
        print("Data written successfully!")
        
        # Read from Delta table
        print("Reading data from Delta table...")
        delta_df = spark.read.format("delta").load(delta_path)
        delta_df.show()
        
        # Update data
        print("Updating data...")
        from delta.tables import DeltaTable
        
        deltaTable = DeltaTable.forPath(spark, delta_path)
        
        # Update age for Alice
        deltaTable.update(
            condition = "name = 'Alice'",
            set = {"age": "26"}
        )
        
        print("Data after update:")
        spark.read.format("delta").load(delta_path).show()
        
        # Show table history
        print("Delta table history:")
        deltaTable.history().show()
        
        # Time travel - read previous version
        print("Reading previous version of the table:")
        spark.read.format("delta").option("versionAsOf", 0).load(delta_path).show()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()