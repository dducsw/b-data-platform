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
    .config("spark.sql.catalog.gravitino", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gravitino.type", "rest") \
    .config("spark.sql.catalog.gravitino.uri", "http://gravitino:8090/api/iceberg/") \
    .config("spark.sql.catalog.gravitino.warehouse", "s3a://warehouse") \
    .config("spark.sql.catalog.gravitino.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.gravitino.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.gravitino.s3.secret-access-key", "minioadmin123") \
    .config("spark.sql.catalog.gravitino.s3.path-style-access", "true") \
    .config("spark.sql.defaultCatalog", "gravitino") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("** Spark session created!")

# 3. Test basic operations
print("** Testing REST catalog...")

try:
    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gravitino.rest_test")

        
    # Create table
    spark.sql("""
            CREATE TABLE IF NOT EXISTS gravitino.rest_test.sample_data (
                id int,
                name string,
                timestamp timestamp
            ) USING iceberg
        """)
    print("✅ Table created")
        
    # Insert data
    spark.sql("""
            INSERT INTO gravitino.rest_test.sample_data VALUES 
            (1, 'rest_test', current_timestamp()),
            (2, 'gravitino', current_timestamp())
        """)
    print("* Data inserted")
        
    # Read data
    count = spark.sql("SELECT COUNT(*) FROM gravitino.rest_test.sample_data").collect()[0][0]
    print(f"* Records: {count}")
        
    # Show sample
    print("* Sample data:")
    spark.sql("SELECT * FROM gravitino.rest_test.sample_data").show()
        
except Exception as e:
    print(f"* Error: {str(e)}")
    raise
finally:
    spark.stop()