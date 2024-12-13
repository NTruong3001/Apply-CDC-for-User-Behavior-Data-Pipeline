from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, sum, avg, when, lit, current_date
from pyspark.sql.types import TimestampType
import uuid
from datetime import datetime, timezone
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import time

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("ETL Cassandra to MySQL with SCD Type 2") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.jars", "E:/de_thay LOng/lab/data ETL pipeline project_cdc/mssql-jdbc-12.8.1.jre11.jar") \
    .config("spark.local.dir", "E:/spark-temp") \
    .getOrCreate()

SQL_USERNAME = 'root'
SQL_PASSWORD = '123'
SQL_DBNAME = 'test'
SQL_SERVERNAME = 'localhost:3306'  # Default SQL Server port
TABLENAME = 'dbo.test'
url = f"jdbc:sqlserver://{SQL_SERVERNAME};databaseName={SQL_DBNAME};encrypt=false"

# SQL Server properties
sqlserver_properties = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'Study_Data_Engineering').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(url,driver,user,password):    
    sql = """(select max(latest_modified_time) from events_etl) data"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 