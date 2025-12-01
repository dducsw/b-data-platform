from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unbase64
from pyspark.sql.types import BinaryType, StringType, StructType
from pyspark.sql.protobuf.functions import from_protobuf
import base64

# docker exec -it spark-master wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar
# docker exec -it spark-worker-1 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar
# docker exec -it spark-worker-2 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-protobuf_2.12/3.5.5/spark-protobuf_2.12-3.5.5.jar" -O /opt/spark/jars/spark-protobuf_2.12-3.5.5.jar


# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("DecodeProtobufBase64") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Đọc file Parquet
df = spark.read.parquet("data/base64_10M.parquet")

df = df.withColumn("data_bytes", unbase64("raw_base64"))

# Decode protobuf
decoded_df = df.withColumn(
    "decoded",
    from_protobuf(
        col("data_bytes"),
        messageName="UFMS.BaseMessage",
        descFilePath="data/ufms.desc"
    )
)

# Hiển thị kết quả
decoded_df.select("decoded.*").limit(100).show(truncate=False)
decoded_df.select("decoded.msgType").distinct().show()
# Sau khi đã có decoded_df như hướng dẫn trước
# decoded_df có trường 'msgType' và các trường con

# Phân loại theo msgType

# Đúng filter theo tên enum chuỗi
df_buswaypoint = decoded_df.filter(col("decoded.msgType") == "MsgType_BusWayPoint").select("decoded.msgBusWayPoint.*")
df_waypoint = decoded_df.filter(col("decoded.msgType") == "MsgType_WayPoint").select("decoded.msgWayPoint.*")
df_buswaypoints = decoded_df.filter(col("decoded.msgType") == "MsgType_BusWayPoints").select("decoded.msgBusWayPoints.*")
df_buswaypointbatch = decoded_df.filter(col("decoded.msgType") == "MsgType_BusWayPointBatch").select("decoded.msgBusWayPointBatch.*")
df_studentcheckin = decoded_df.filter(col("decoded.msgType") == "MsgType_StudentCheckInPoint").select("decoded.msgStudentCheckInPoint.*")
df_regvehicle = decoded_df.filter(col("decoded.msgType") == "MsgType_RegVehicle").select("decoded.msgRegVehicle.*")
df_regdriver = decoded_df.filter(col("decoded.msgType") == "MsgType_RegDriver").select("decoded.msgRegDriver.*")
df_regcompany = decoded_df.filter(col("decoded.msgType") == "MsgType_RegCompany").select("decoded.msgRegCompany.*")
df_busticketpoint = decoded_df.filter(col("decoded.msgType") == "MsgType_BusTicketPoint").select("decoded.msgBusTicketPoint.*")
df_busticketstartend = decoded_df.filter(col("decoded.msgType") == "MsgType_BusTicketStartEndPoint").select("decoded.msgBusTicketStartEndPoint.*")

# Ví dụ: Hiển thị 5 dòng đầu của từng loại
print("--- WayPoint Data ---")
df_waypoint.show(5)
print("--- BusWayPoint Data ---")
df_buswaypoint.show(5)
df_buswaypoints.show(5)
df_buswaypointbatch.show(5)
df_studentcheckin.show(5)
df_regvehicle.show(5)
print("--- Driver Data ---")
df_regdriver.show(5)
df_regcompany.show(5)
print("--- Company Data ---")
df_regcompany.show(5)
df_busticketpoint.show(5)
df_busticketstartend.show(5)
# ... các loại khác tương tự

# Có thể lưu từng loại ra file riêng nếu cần
# df_waypoint.write.parquet("data/waypoint.parquet")
# Nếu muốn lưu ra file CSV/Parquet
# decoded_df.select("decoded.*").write.csv("data/decoded_output.csv", header=True)
# decoded_df.select("decoded.*").write.parquet("data/decoded_output.parquet")