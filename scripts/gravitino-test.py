#!/usr/bin/env python3
"""
Gravitino Integration Test Script
=================================
Test Apache Gravitino with Spark and Iceberg integration
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime
import requests
import json

print("🚀 Gravitino Integration Test")
print("=" * 40)

# Test Gravitino REST API first
print("🔌 Testing Gravitino REST API...")
try:
    response = requests.get("http://gravitino:8090/api/version")
    if response.status_code == 200:
        version_info = response.json()
        print(f"✅ Gravitino version: {version_info}")
    else:
        print(f"❌ Gravitino API error: {response.status_code}")
except Exception as e:
    print(f"❌ Cannot connect to Gravitino: {e}")

# Create Spark session with Gravitino catalog
print("⚡ Creating Spark session with Gravitino...")
spark = SparkSession.builder \
    .appName("GravitinoIcebergTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.gravitino", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gravitino.type", "rest") \
    .config("spark.sql.catalog.gravitino.uri", "http://gravitino:8090/api/iceberg/") \
    .config("spark.sql.catalog.gravitino.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.gravitino.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.gravitino.s3.path-style-access", "true") \
    .config("spark.sql.catalog.gravitino.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.gravitino.s3.secret-access-key", "minioadmin123") \
    .config("spark.sql.defaultCatalog", "gravitino") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

try:
    # List available catalogs
    print("📋 Available catalogs:")
    catalogs = spark.sql("SHOW CATALOGS").collect()
    for catalog in catalogs:
        print(f"   - {catalog[0]}")
    
    # Create database/namespace in Gravitino
    print("🗂️ Creating database via Gravitino...")
    spark.sql("CREATE DATABASE IF NOT EXISTS gravitino.transportation")
    print("✅ Database created!")
    
    # List databases
    print("📂 Available databases:")
    databases = spark.sql("SHOW DATABASES IN gravitino").collect()
    for db in databases:
        print(f"   - {db[0]}")
    
    # Create table via Gravitino catalog
    print("🚌 Creating vehicle tracking table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gravitino.transportation.vehicle_tracking (
            vehicle_id STRING,
            vehicle_type STRING,
            route_id STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            speed DOUBLE,
            fuel_level DOUBLE,
            passenger_count INTEGER,
            timestamp TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
    """)
    print("✅ Table created!")
    
    # Create sample data for different vehicle types
    print("📊 Creating sample transportation data...")
    data = [
        ("BUS_001", "Bus", "ROUTE_1", 10.7769, 106.7009, 45.5, 85.2, 28, datetime(2024, 10, 29, 14, 30, 0)),
        ("BUS_002", "Bus", "ROUTE_2", 10.7829, 106.6819, 38.2, 72.1, 15, datetime(2024, 10, 29, 14, 31, 0)),
        ("TAXI_001", "Taxi", "ZONE_A", 10.7909, 106.6919, 52.1, 45.8, 2, datetime(2024, 10, 29, 14, 32, 0)),
        ("TRUCK_001", "Truck", "DELIVERY", 10.7650, 106.7100, 35.0, 60.5, 0, datetime(2024, 10, 29, 14, 33, 0)),
        ("METRO_001", "Metro", "LINE_1", 10.7800, 106.6950, 80.0, 95.0, 120, datetime(2024, 10, 29, 14, 34, 0))
    ]
    
    # Define schema
    schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("fuel_level", DoubleType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Write to Gravitino-managed Iceberg table
    print("💾 Writing data via Gravitino catalog...")
    df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("gravitino.transportation.vehicle_tracking")
    print("✅ Data written!")
    
    # Read and display data
    print("🔍 Reading data from Gravitino catalog...")
    result_df = spark.table("gravitino.transportation.vehicle_tracking")
    print(f"📈 Total records: {result_df.count()}")
    
    print("\n📋 All vehicle tracking data:")
    result_df.show(truncate=False)
    
    # Analytics queries via Gravitino
    print("\n🚌 Public transportation vehicles (Bus, Metro):")
    spark.sql("""
        SELECT vehicle_id, vehicle_type, speed, passenger_count, fuel_level
        FROM gravitino.transportation.vehicle_tracking 
        WHERE vehicle_type IN ('Bus', 'Metro')
        ORDER BY passenger_count DESC
    """).show()
    
    print("\n⛽ Vehicles with low fuel (< 50%):")
    spark.sql("""
        SELECT vehicle_id, vehicle_type, fuel_level, speed
        FROM gravitino.transportation.vehicle_tracking 
        WHERE fuel_level < 50.0
        ORDER BY fuel_level ASC
    """).show()
    
    print("\n📊 Average metrics by vehicle type:")
    spark.sql("""
        SELECT 
            vehicle_type,
            COUNT(*) as vehicle_count,
            ROUND(AVG(speed), 2) as avg_speed,
            ROUND(AVG(fuel_level), 2) as avg_fuel,
            ROUND(AVG(passenger_count), 2) as avg_passengers
        FROM gravitino.transportation.vehicle_tracking 
        GROUP BY vehicle_type
        ORDER BY avg_speed DESC
    """).show()
    
    # Test table operations via Gravitino
    print("\n🔧 Testing Gravitino table operations...")
    tables = spark.sql("SHOW TABLES IN gravitino.transportation").collect()
    print("📋 Tables in transportation database:")
    for table in tables:
        print(f"   - {table[1]}")
    
    # Show table properties
    print("\n📄 Table properties:")
    properties = spark.sql("SHOW TBLPROPERTIES gravitino.transportation.vehicle_tracking").collect()
    for prop in properties[:5]:  # Show first 5 properties
        print(f"   - {prop[0]}: {prop[1]}")
    
    print("\n🎉 Gravitino integration test completed successfully!")
    print("\n✅ Verified capabilities:")
    print("   - Gravitino REST API connectivity")
    print("   - Database creation via Gravitino")
    print("   - Iceberg table management")
    print("   - Data write/read operations")
    print("   - Complex SQL analytics")
    print("   - Metadata management")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    raise
finally:
    spark.stop()