from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf, date_format, col, when, sum, avg
from pyspark.sql.types import TimestampType
import uuid
import time
from datetime import datetime, timezone
from pyspark.sql.functions import max
import mysql.connector
# Hàm tạo SparkSession
spark = SparkSession.builder \
    .appName("ETL Cassandra to MySQL with SCD Type 2") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,mysql:mysql-connector-java:8.0.28") \
    .config("spark.local.dir", "E:/spark-temp")\
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .config("spark.cleaner.referenceTracking.blocking", "false") \
    .getOrCreate()

# Cấu hình kết nối MySQL
host = 'localhost'
port = '3306'
db_name = 'test'
user = 'root'
password = '123'
url = f'jdbc:mysql://{host}:{port}/{db_name}'
driver = "com.mysql.cj.jdbc.Driver"

# Hàm thêm cột ID
def general_id(df):
    return df.withColumn("ID", monotonically_increasing_id())

# Hàm chuyển UUID thành thời gian datetime
def uuid_to_datetime(uuid_v1):
    uuid_obj = uuid.UUID(uuid_v1)
    timestamp = (uuid_obj.time - 0x01B21DD213814000) / 1e7
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt

uuid_to_datetime_udf = udf(uuid_to_datetime, TimestampType())

# Hàm thêm cột thời gian vào dataframe
def add_collum_date_time(df):
    return df \
        .withColumn("dates", date_format(uuid_to_datetime_udf("create_time"), "yyyy-MM-dd")) \
        .withColumn("hours", date_format(uuid_to_datetime_udf("create_time"), "HH:mm:ss"))

# Hàm thêm các cột tùy chỉnh vào dataframe
def add_custom_track_columns(df):
    return df \
        .withColumn("disqualified_application", when(col("custom_track") == "null", 1).otherwise(0)) \
        .withColumn("qualified_application", when(col("custom_track") == "qualified", 1).otherwise(0)) \
        .withColumn("conversion", when(col("custom_track") == "conversion", 1).otherwise(0)) \
        .withColumn("clicks", when(col("custom_track") == "clicks", 1).otherwise(0))

# Hàm xử lý dữ liệu
def process_data():
    # Reading from Cassandra (to ensure fresh data)
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="mytable", keyspace="my_keyspace") \
        .load()

    # Thêm cột ID
    result_1 = general_id(data)
    
    # Thêm cột thời gian
    result_2 = add_collum_date_time(result_1)
    
    # Thêm các cột tùy chỉnh
    result_3 = add_custom_track_columns(result_2)

    # Nhóm và tính toán các giá trị tổng hợp
    final_result = result_3.groupBy(
        'ID', 'job_id', 'dates', 'hours', 'disqualified_application', 
        'qualified_application', 'conversion', 
        'group_id', 'campaign_id', 'clicks'
    ).agg(
        sum("bid").alias("spend_hour"),
        avg("bid").alias("bid_set")
    )

    # Write to MySQL
    final_result.write.format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("dbtable", "mytable") \
        .mode("append") \
        .option("user", user) \
        .option("password", password) \
        .save()

def get_latest_create_time_cassandra():
    # Reading data from Cassandra table
    data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="mytable", keyspace="my_keyspace") \
        .load()

    # Retrieve the latest 'create_time' (assuming 'create_time' is the name of the column)
    latest_create_time = uuid_to_datetime(data.agg(max("create_time").alias("latest_create_time")).collect()[0]["latest_create_time"])

    return latest_create_time.strftime("%Y-%m-%d  %H:%M:%S")

def lastest_date_from_mysql(host, port, db_name, user, password):
    try:
        # Kết nối đến cơ sở dữ liệu MySQL
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db_name
        )
        
        # Tạo con trỏ để thực thi truy vấn
        cursor = conn.cursor()
        
        # Thực hiện truy vấn SQL để lấy giờ và ngày gần nhất
        cursor.execute("""
                       SELECT hours, dates
                       FROM mytable
                       ORDER BY dates DESC, hours DESC
                       LIMIT 1;
                       """)
        
        # Lấy kết quả
        result = cursor.fetchone()
        
        if result:
            # Kết hợp ngày và giờ
            date_time_str = f"{result[1]} {result[0]}"  # dates + hours
            date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return date_time_obj.strftime("%Y-%m-%d  %H:%M:%S")
        else:
            return None

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    
    finally:
        # Đóng kết nối
        cursor.close()
        conn.close()


while True :
    start_time = datetime.now()
    cassandra_time = get_latest_create_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = lastest_date_from_mysql(host, port, db_name, user, password)
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        print("Do main task")
        process_data()
    else :
        print("No new data found")
    end_time =datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)