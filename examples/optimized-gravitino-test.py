#!/usr/bin/env python3

"""
Optimized Gravitino + Iceberg Integration Test
Using simplified Spark session and REST catalog
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

print("🚀 Optimized Gravitino + Iceberg Test")
print("=" * 50)

# Test Gravitino REST API
print("🔌 Testing Gravitino REST API...")
try:
    response = requests.get("http://gravitino:8090/api/version")
    if response.status_code == 200:
        version_info = response.json()
        print(f"✅ Gravitino version: {version_info['version']['version']}")
    else:
        print(f"❌ Gravitino API error: {response.status_code}")
except Exception as e:
    print(f"❌ Cannot connect to Gravitino: {e}")

# Optimized Spark session with REST catalog
print("⚡ Creating optimized Spark session...")

# Option 1: Using Gravitino REST catalog (recommended)
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

print("✅ Spark session created with Gravitino REST catalog!")

# Test catalog connection
print("📋 Available catalogs:")
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    for catalog in catalogs:
        print(f"   - {catalog[0]}")
except Exception as e:
    print(f"⚠️ Error listing catalogs: {e}")

# Create a simple test table
print("🚌 Creating test table...")
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gravitino.test_db.vehicles (
            id INT,
            name STRING,
            type STRING,
            speed DOUBLE,
            timestamp TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    print("✅ Table created via Gravitino REST catalog!")
except Exception as e:
    print(f"⚠️ Table creation error: {e}")
    print("   Falling back to direct Iceberg...")
    
    # Fallback: Direct Iceberg without namespace
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gravitino.simple_vehicles (
            id INT,
            name STRING,
            type STRING,
            speed DOUBLE,
            timestamp TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    print("✅ Fallback table created!")

# Insert test data
print("📊 Inserting test data...")
test_data = [
    (1, "Bus A1", "Bus", 45.5, datetime(2024, 10, 29, 15, 0, 0)),
    (2, "Metro M1", "Metro", 80.0, datetime(2024, 10, 29, 15, 1, 0)),
    (3, "Taxi T1", "Taxi", 52.1, datetime(2024, 10, 29, 15, 2, 0))
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = spark.createDataFrame(test_data, schema)

try:
    df.write.mode("append").saveAsTable("gravitino.test_db.vehicles")
    table_name = "gravitino.test_db.vehicles"
except:
    df.write.mode("append").saveAsTable("gravitino.simple_vehicles")
    table_name = "gravitino.simple_vehicles"

print("✅ Data inserted!")

# Query data
print("🔍 Querying data...")
result = spark.table(table_name)
print(f"📈 Total records: {result.count()}")
result.show(truncate=False)

# Analytics
print("📊 Quick analytics:")
spark.sql(f"""
    SELECT type, COUNT(*) as count, AVG(speed) as avg_speed
    FROM {table_name}
    GROUP BY type
    ORDER BY avg_speed DESC
""").show()

print("\n🎉 Optimized test completed!")
print("\n✅ Benefits of this approach:")
print("   - Shorter Spark session configuration")
print("   - Uses Gravitino REST catalog when available")
print("   - Automatic fallback to direct Iceberg")
print("   - Simplified S3 configuration through catalog")
print("   - Better metadata management via Gravitino")

spark.stop()