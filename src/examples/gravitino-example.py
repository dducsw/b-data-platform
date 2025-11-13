#!/usr/bin/env python3
"""
REST Catalog Test - Shorter Spark Session Configuration
"""

from pyspark.sql import SparkSession
import time

print("** Creating Spark session with REST catalog...")

spark = SparkSession.builder \
    .appName("OptimizedGravitinoTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.catalog_rest", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.catalog_rest.type", "rest") \
    .config("spark.sql.catalog.catalog_rest.uri", "http://gravitino:9001/iceberg/") \
    .config("spark.sql.catalog.catalog_rest.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.catalog_rest.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.catalog_rest.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.catalog_rest.s3.secret-access-key", "minioadmin123") \
    .config("spark.sql.catalog.catalog_rest.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("** Spark session created!")

# Test basic operations
print("** Testing REST catalog...")

try:
    # Use the correct catalog name
    spark.sql("USE catalog_rest")

    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_rest.test1")

    # Create table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_rest.test1.sample_data (
            id INT,
            name STRING,
            timestamp TIMESTAMP
        ) USING iceberg
    """)
    print("Table created")

    # Insert data
    spark.sql("""
        INSERT INTO catalog_rest.test1.sample_data VALUES 
        (1, 'rest_test', current_timestamp()),
        (2, 'gravitino', current_timestamp())
    """)
    print("* Data inserted")

    # Read data
    count = spark.sql("SELECT COUNT(*) FROM catalog_rest.test1.sample_data").collect()[0][0]
    print(f"* Records: {count}")

    # Show sample
    print("* Sample data:")
    spark.sql("SELECT * FROM catalog_rest.test1.sample_data").show()

except Exception as e:
    print(f"* Error: {str(e)}")
    raise
finally:
    spark.stop()