import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, asc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Get environment variables or default values
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
NAMENODE_URL = os.getenv('NAMENODE_URL', 'hdfs://localhost:9000')

# Define the schema matching your aggregated candle data structure
candle_schema = StructType([
    StructField("window_start", TimestampType()),
    StructField("window_end", TimestampType()),
    StructField("instrument", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("buy_volume", LongType()),
    StructField("sell_volume", LongType()),
    StructField("total_volume", LongType()),
    StructField("delta", LongType()),
    StructField("tbq", DoubleType()),
    StructField("tsq", DoubleType())
])

def main():
    # Initialize Spark session with Hive support if needed
    spark = SparkSession.builder \
        .appName("CandlesToHDFS") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"✓ Connecting to Kafka topic: upstox_orderflow at {KAFKA_BROKERS}")

    # Read raw JSON candle data from Kafka topic
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", "upstox_orderflow") \
        .option("startingOffsets", "latest") \
        .load()

    # Cast value to string and parse JSON according to schema
    candles_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), candle_schema).alias("data")) \
        .select("data.*")

    # Add a date partition column (date of window_start)
    candles_with_date = candles_df.withColumn("date", to_date(col("window_start")))

    # Order candles by timestamp within each micro-batch (can't guarantee global order in streaming)
    ordered_candles = candles_with_date.orderBy(asc("window_start"))

    # Write stream to Parquet files partitioned by date, append mode, with checkpoints for fault tolerance
    query = ordered_candles.writeStream \
        .format("parquet") \
        .option("path", f"{NAMENODE_URL}/trading/candles") \
        .option("checkpointLocation", f"{NAMENODE_URL}/checkpoints/candles") \
        .partitionBy("date") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    print("✓ Candle streaming started. Writing to:", f"{NAMENODE_URL}/trading/candles")

    query.awaitTermination()

if __name__ == "__main__":
    main()

