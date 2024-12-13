from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, date_format,when,col, sum,avg
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Spark Cassandra Example") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.jars", "E:/de_thay LOng/lab/data ETL pipeline project/mssql-jdbc-12.8.1.jre11.jar") \
    .config("spark.local.dir", "E:/spark-temp") \
    .getOrCreate()

# Đọc dữ liệu từ Cassandra
data = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="mytable", keyspace="my_keyspace") \
    .load()

# Hàm thêm cột ID
def general_id(df):
    return df.withColumn("ID", monotonically_increasing_id())

def uuid_to_datetime(uuid_v1):
    uuid_obj = uuid.UUID(uuid_v1)
    timestamp = (uuid_obj.time - 0x01B21DD213814000) / 1e7
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt
uuid_to_datetime_udf = udf(uuid_to_datetime, TimestampType()) 
 
def add_collum_date_time(df):
    result = df \
        .withColumn("dates", date_format(uuid_to_datetime_udf("create_time"), "yyyy-MM-dd")) \
        .withColumn("hours", date_format(uuid_to_datetime_udf("create_time"), "HH:mm:ss"))
    return result
def add_custom_track_columns(df):
    result = df \
        .withColumn("disqualified_application", when(col("custom_track") == "null", 1).otherwise(0)) \
        .withColumn("qualified_application", when(col("custom_track") == "qualified", 1).otherwise(0)) \
        .withColumn("conversion", when(col("custom_track") == "conversion", 1).otherwise(0)) \
        .withColumn("clicks", when(col("custom_track") == "clicks", 1).otherwise(0))
    return result
result_1 = general_id(data)
result_2= add_collum_date_time(result_1)
result_3= add_custom_track_columns(result_2)

final_result = result_3.groupBy(
    'ID', 'job_id', 'dates', 'hours', 'disqualified_application', 
    'qualified_application', 'conversion', 
    'group_id', 'campaign_id', 'clicks'
).agg(
    sum("bid").alias("spend_hour"),
    avg("bid").alias("bid_set")
)


SQL_USERNAME = 'sa'
SQL_PASSWORD = '123'
SQL_DBNAME = 'test'
SQL_SERVERNAME = 'localhost:1433'  # Default SQL Server port
TABLENAME = 'dbo.mytable'
url = f"jdbc:sqlserver://{SQL_SERVERNAME};databaseName={SQL_DBNAME};encrypt=false"

# SQL Server properties
sqlserver_properties = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


final_result.write.jdbc(url=url, table=TABLENAME, mode="overwrite", properties=sqlserver_properties)

