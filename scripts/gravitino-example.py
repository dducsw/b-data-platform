#!/usr/bin/env python3
"""
REST Catalog Test - Shorter Spark Session Configuration
"""

from pyspark.sql import SparkSession
import requests

# Check if the Iceberg REST server is reachable
try:
    response = requests.get("http://gravitino:9001/iceberg/v1/config")
    if response.status_code == 200:
        print("Iceberg REST server is reachable.")
    else:
        print(f"Unexpected response from Iceberg REST server: {response.status_code}")
        exit(1)
except requests.ConnectionError:
    print("Error: Unable to connect to the Iceberg REST server at http://gravitino:9001")
    exit(1)

print("** Creating Spark session with REST catalog...")

spark = SparkSession.builder \
    .appName("OptimizedGravitinoTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("** Spark session created!")

# Test basic operations
print("** Testing REST catalog...")

try:
    # Use the correct catalog
    spark.sql("USE my_catalog")
    print("USE CATALOG SUCESS")
    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test1")
    print("CREATE SUCESS")
    # Create table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS test1.sample_data (
            id INT,
            data STRING,
            create_time TIMESTAMP
        ) USING iceberg
    """)
    print("** Table created!")

    # Insert data
    spark.sql("""
        INSERT INTO test1.sample_data VALUES 
        (1, 'test1', current_timestamp()),
        (2, 'gravitino', current_timestamp())
    """)
    print("** Data inserted!")

    # Query data
    spark.sql("SELECT * FROM test1.sample_data").show()

except Exception as e:
    print(f"* Error: {str(e)}")
    raise
finally:
    spark.stop()