import signal
import sys
import time
import uuid  # Import the uuid module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, format_number
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'user_interactions'
CASSANDRA_TABLE = 'events_raw_data'

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'user_interactions'
MYSQL_TABLE = 'aggregated_interactions'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'root'

KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'user_interactions'
table_user = "aggregated_interactions_by_user"
table_agg_sum ="agg_user_timestamp"
table_event = "aggregated_interactions_by_event"


def write_to_cassandra(df, table_name):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
      .mode("append") \
      .save()
      
def write_to_mysql(df, epoch_id):
    # Write DataFrame to MySQL table
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}") \
        .option("dbtable", table_agg_sum) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .save()

def save_to_mysql(df, table_name):
    df.write \
      .jdbc(url=f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", 
            table=table_name, 
            mode="append", 
            properties={
                "driver": "com.mysql.cj.jdbc.Driver",
                "user": MYSQL_USERNAME,
                "password": MYSQL_PASSWORD
            })

def write_to_mysql_tmp(df, epoch_id):
    # Aggregation by user
    agg_df_by_user = df.withColumn("interaction_date", to_date(col("timestamp"))) \
                       .withColumn("event_id", col("event_id").cast(StringType())) \
                       .groupBy("interaction_date", "user_id", "event_type", "event_id") \
                       .count() \
                       .withColumnRenamed("count", "interaction_count") \
                       .withColumn("processed_at", current_timestamp())
                       
    save_to_mysql(agg_df_by_user, table_user) 
    
    # Aggregation by event
    agg_df_by_event = df.withColumn("interaction_date", to_date(col("timestamp"))) \
                        .withColumn("event_id", col("event_id").cast(StringType())) \
                        .groupBy("interaction_date", "event_type", "event_id") \
                        .count() \
                        .withColumnRenamed("count", "interaction_count") \
                        .withColumn("processed_at", current_timestamp())
                        
    save_to_mysql(agg_df_by_event, table_event) 
                        

def signal_handler(signal, frame):
    print("Waiting for 90 seconds before terminating the Spark Streaming application...")
    time.sleep(90)
    sys.exit(0)

def main():
    
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra-MySQL-ETL") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("event_id", StringType(), True),  # Include event_id in the schema
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("page_id", IntegerType(), True)
    ])
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema=schema).alias("data"), col("timestamp")) \
    .select("data.*", col("timestamp").alias("event_timestamp"))  # Rename the timestamp column to avoid ambiguity

    summary_df = df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy("user_id", "event_type", window("event_timestamp", "2 minutes", "1 minutes")) \
        .agg(count("event_id").alias("interaction_count")) \
        .select("user_id",
                "event_type",
                format_number(col("interaction_count"), 0).alias("interaction_count"),
                "window.start",
                "window.end")
        
    # Write the summarized data to the console
    streaming_query = summary_df.writeStream \
        .queryName("user_interaction_summary") \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    df.createOrReplaceTempView("tmp_table")

    query = """
        SELECT *
        FROM tmp_table
    """
    tmp_df = spark.sql(query)
                       
    agg_df_by_user_write =  tmp_df.writeStream \
        .foreachBatch(write_to_mysql_tmp) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
      
    tmp_df.select("user_id","event_id", "event_type","timestamp", "page_id").writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    signal.signal(signal.SIGINT, signal_handler)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
