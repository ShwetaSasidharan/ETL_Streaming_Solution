import signal
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'user_interactions'
CASSANDRA_TABLE = 'events'

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'user_interactions'
MYSQL_TABLE = 'aggregated_interactions'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'root'

KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'user_interactions'

def write_to_mysql(df, epoch_id):
    agg_df_by_user = df.withColumn("interaction_date", to_date(col("timestamp"))) \
                       .groupBy("interaction_date", "user_id", "event_type") \
                       .count() \
                       .withColumnRenamed("count", "interaction_count") \
                       .withColumn("processed_at", current_timestamp())

    agg_df_by_user.write \
                  .jdbc(url=f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", 
                        table="aggregated_interactions_by_user", 
                        mode="append", 
                        properties={
                            "driver": "com.mysql.cj.jdbc.Driver",
                            "user": MYSQL_USERNAME,
                            "password": MYSQL_PASSWORD
                        })
    
    agg_df_by_event = df.withColumn("interaction_date", to_date(col("timestamp"))) \
                        .groupBy("interaction_date", "event_type") \
                        .count() \
                        .withColumnRenamed("count", "interaction_count") \
                        .withColumn("processed_at", current_timestamp())

    agg_df_by_event.write \
                   .jdbc(url=f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", 
                         table="aggregated_interactions_by_event", 
                         mode="append", 
                         properties={
                             "driver": "com.mysql.cj.jdbc.Driver",
                             "user": MYSQL_USERNAME,
                             "password": MYSQL_PASSWORD
                         })

def signal_handler(signal, frame):
    print("Waiting for 90 seconds before terminating the Spark Streaming application...")
    time.sleep(90)
    sys.exit(0)

def main():
    
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra-MySQL") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("page_id", IntegerType(), True)
    ])

    spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .createOrReplaceTempView("tmp_table")

    query = """
        SELECT *
        FROM tmp_table
    """

    tmp_df = spark.sql(query)

    tmp_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    signal.signal(signal.SIGINT, signal_handler)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
