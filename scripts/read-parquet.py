import pyspark
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
        .appName("ReadParquet")
        .getOrCreate()
)


TABLE = "iceberg.demo.bus_gps_demo"
data = spark.read.format("parquet").load("s3a://warehouse/demo/bus_gps_demo/data/")

print(f"🔍 Reading Iceberg table: {TABLE}")

df = spark.read.format("iceberg").load(TABLE)

print("📌 Schema:")
df.printSchema()

print("\n📌 Sample data:")
df.show(50, truncate=False)

spark.stop()
