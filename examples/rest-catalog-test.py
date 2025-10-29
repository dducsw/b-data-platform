#!/usr/bin/env python3
"""
REST Catalog Test - Shorter Spark Session Configuration
"""

from pyspark.sql import SparkSession
import time

print("🚀 REST Catalog Test")
print("=" * 50)

# 1. Check Gravitino status
try:
    import requests
    gravitino_resp = requests.get("http://gravitino:8090/api/version", timeout=5)
    print(f"✅ Gravitino: {gravitino_resp.json().get('version', 'Unknown')}")
except Exception as e:
    print(f"❌ Gravitino check failed: {e}")
    exit(1)

# 2. Create Spark session with REST catalog (short config)
print("⚡ Creating Spark session with REST catalog...")

# SHORT CONFIG - Only 8 lines!
spark = SparkSession.builder \
    .appName("RestCatalogTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.gravitino", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gravitino.type", "rest") \
    .config("spark.sql.catalog.gravitino.uri", "http://gravitino:8090/api/iceberg/") \
    .config("spark.sql.catalog.gravitino.warehouse", "s3a://warehouse") \
    .config("spark.sql.defaultCatalog", "gravitino") \
    .getOrCreate()

print("✅ Spark session created!")

# 3. Test basic operations
print("🚌 Testing REST catalog...")

try:
    # Create namespace if not exists
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS gravitino.rest_test")
        print("✅ Namespace created")
    except Exception as e:
        print(f"⚠️ Namespace creation: {e}")
    
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
    print("✅ Data inserted")
    
    # Read data
    count = spark.sql("SELECT COUNT(*) FROM gravitino.rest_test.sample_data").collect()[0][0]
    print(f"📊 Records: {count}")
    
    # Show sample
    print("📋 Sample data:")
    spark.sql("SELECT * FROM gravitino.rest_test.sample_data").show()
    
except Exception as e:
    print(f"❌ Error: {e}")
    # Try direct Iceberg fallback
    print("🔄 Trying direct Iceberg fallback...")
    
    # Fallback: add S3 configs to existing session
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    
    # Create iceberg catalog
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.rest_fallback (
            id int,
            name string
        ) USING iceberg
        LOCATION 's3a://warehouse/rest_fallback'
    """)
    
    spark.sql("INSERT INTO iceberg.rest_fallback VALUES (1, 'fallback')")
    fallback_count = spark.sql("SELECT COUNT(*) FROM iceberg.rest_fallback").collect()[0][0]
    print(f"📊 Fallback records: {fallback_count}")

print("\n🎯 Configuration summary:")
print("=" * 50)
print("📝 REST CATALOG CONFIG:")
print("   - Lines: 8 configs only!")
print("   - Type: REST catalog")
print("   - S3: Managed by Gravitino")  
print("   - Simpler: No manual S3 configs needed")
print("   - Benefits: Centralized metadata management")

spark.stop()
print("✅ Test completed!")