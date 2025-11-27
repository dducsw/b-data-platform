from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadDecoded").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Read CSV ===")
df_csv = spark.read.option("header", "true").csv("/opt/spark/data/decoded_busgps_csv_udf")
df_csv.show(5, truncate=False)

print("\n=== Read Parquet ===")
df_pq = spark.read.parquet("/opt/spark/data/decoded_busgps_parquet_udf")
df_pq.show(5, truncate=False)
df_pq.printSchema()
