from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, expr
from pyspark.sql.types import StringType, StructType, StructField, MapType

# Initialize Spark Session with Hive and HDFS support
spark = SparkSession.builder \
    .appName("TickFullModeToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for the full mode tick data as a raw JSON string (keep flexible here)
# We'll treat the entire tick JSON as a string so it's stored verbatim.

# Kafka configuration
kafka_bootstrap_servers = "kafka:29092"  # Use 'localhost:9092' if running locally without Docker
kafka_topic = "upstox_ticks"

# Read streaming JSON ticks from Kafka
kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Cast the binary 'value' column to string JSON
raw_ticks_df = kafka_raw_df.selectExpr("CAST(value AS STRING) as json_str")

# Define schema for only extracting timestamp for ordering and partitions
# Using flexible schema: parse only the timestamp field 'feeds'->{instrument}->fullFeed->marketFF->ltpc->ltt
tick_timestamp_schema = StructType([
    StructField("feeds", MapType(StringType(), StructType([
        StructField("fullFeed", StructType([
            StructField("marketFF", StructType([
                StructField("ltpc", StructType([
                    StructField("ltt", StringType())
                ]))
            ]))
        ]))
    ])))
])

# Parse JSON to extract timestamp for ordering
ticks_with_ts = raw_ticks_df \
    .withColumn("json_data", from_json(col("json_str"), tick_timestamp_schema)) \
    .withColumn("instrument", expr("map_keys(json_data.feeds)[0]")) \
    .withColumn("ltpc", expr("json_data.feeds[instrument].fullFeed.marketFF.ltpc")) \
    .withColumn("ltt_str", col("ltpc.ltt")) \
    .withColumn("tick_time", to_timestamp((col("ltt_str") / 1000).cast("long"))) \
    .withColumn("date", to_date("tick_time"))

# Select columns for storage; store the complete raw JSON string and the extracted timestamp & instrument for partitioning and sorting
ticks_to_store = ticks_with_ts.select(
    col("json_str"),
    col("instrument"),
    col("tick_time"),
    col("date")
)

# Write stream to HDFS as Parquet partitioned by date; order in files by tick_time
query = ticks_to_store \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/trading/ticks") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/ticks") \
    .partitionBy("date") \
    .queryName("TickStreamToHDFS") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✓ Streaming tick data from upstox_ticks topic to HDFS in full mode, partitioned by date")

query.awaitTermination()

