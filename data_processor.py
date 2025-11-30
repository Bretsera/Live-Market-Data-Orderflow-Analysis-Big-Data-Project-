import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, expr, to_json, struct, explode, first, last, max as spark_max, min as spark_min, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, ArrayType, MapType

# Environment variable configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
NAMENODE_URL = os.getenv('NAMENODE_URL', 'hdfs://localhost:9000')
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'local[*]')

spark = SparkSession.builder \
    .master(SPARK_MASTER_URL) \
    .appName("OrderFlowProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

n = 1  # window minutes

schema = StructType([
    StructField("type", StringType()),
    StructField("feeds", MapType(
        StringType(),
        StructType([
            StructField("fullFeed", StructType([
                StructField("marketFF", StructType([
                    StructField("ltpc", StructType([
                        StructField("ltp", DoubleType()),
                        StructField("ltt", StringType()),
                        StructField("ltq", StringType()),
                        StructField("cp", DoubleType())
                    ])),
                    StructField("marketLevel", StructType([
                        StructField("bidAskQuote", ArrayType(StructType([
                            StructField("bidQ", StringType()),
                            StructField("bidP", DoubleType()),
                            StructField("askQ", StringType()),
                            StructField("askP", DoubleType())
                        ])))
                    ])),
                    StructField("optionGreeks", StructType([])),
                    StructField("marketOHLC", StructType([
                        StructField("ohlc", ArrayType(StructType([
                            StructField("interval", StringType()),
                            StructField("open", DoubleType()),
                            StructField("high", DoubleType()),
                            StructField("low", DoubleType()),
                            StructField("close", DoubleType()),
                            StructField("vol", StringType()),
                            StructField("ts", StringType())
                        ])))
                    ])),
                    StructField("atp", DoubleType()),
                    StructField("vtt", StringType()),
                    StructField("tbq", DoubleType()),  # moved here inside marketFF
                    StructField("tsq", DoubleType())   # moved here
                ])),
                StructField("requestMode", StringType())
            ]))
        ])
    )),
    StructField("currentTs", StringType())
])

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "upstox_ticks") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# Optional debug: print raw JSON
json_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .queryName("debug_raw_json") \
    .start()

parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Optional debug: print parsed data
parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .queryName("debug_parsed") \
    .start()

feeds_exploded = parsed.select("type", explode("feeds").alias("instrument", "feed"), "currentTs")

# Optional debug: print exploded feeds
feeds_exploded.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .queryName("debug_exploded") \
    .start()

ticks = feeds_exploded.select(
    col("instrument"),
    col("feed.fullFeed.marketFF.ltpc.ltp").alias("ltp"),
    col("feed.fullFeed.marketFF.ltpc.ltq").alias("ltq"),
    col("feed.fullFeed.marketFF.ltpc.ltt").alias("ltt"),
    col("feed.fullFeed.marketFF.marketLevel.bidAskQuote").alias("bidAskQuote"),
    col("feed.fullFeed.marketFF.tbq").alias("tbq"),
    col("feed.fullFeed.marketFF.tsq").alias("tsq"),
    col("currentTs")
)

ticks = ticks.withColumn("ltq", col("ltq").cast(IntegerType())) \
    .withColumn("ltt_long", col("ltt").cast(LongType())) \
    .withColumn("event_time", (col("ltt_long") / 1000).cast("timestamp")) \
    .filter(col("ltq").isNotNull() & col("ltp").isNotNull() & col("event_time").isNotNull())

# Optional debug: print filtered ticks
ticks.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .queryName("debug_ticks") \
    .start()

ticks = ticks.withColumn("bid_prices", expr("transform(bidAskQuote, x -> x.bidP)")) \
    .withColumn("ask_prices", expr("transform(bidAskQuote, x -> x.askP)"))

MAX_DOUBLE = 1.7976931348623157E308
MIN_DOUBLE = -1.7976931348623157E308

ticks = ticks.withColumn("best_bid", expr(
    f"aggregate(bid_prices, cast({MIN_DOUBLE} as double), (acc, x) -> IF(acc > x, acc, x))")) \
    .withColumn("best_ask", expr(
    f"aggregate(ask_prices, cast({MAX_DOUBLE} as double), (acc, x) -> IF(acc < x, acc, x))"))

ticks = ticks.withColumn("buy_volume",
    expr("CASE WHEN abs(ltp - best_ask) <= abs(ltp - best_bid) THEN ltq ELSE 0 END")) \
    .withColumn("sell_volume",
    expr("CASE WHEN abs(ltp - best_bid) < abs(ltp - best_ask) THEN ltq ELSE 0 END")) \
    .withColumn("delta", col("buy_volume") - col("sell_volume"))

ticks_with_watermark = ticks.withWatermark("event_time", "5 minutes")

agg_metrics = ticks_with_watermark.groupBy(
    window(col("event_time"), f"{n} minutes"),
    col("instrument")
).agg(
    first("ltp").alias("open"),
    spark_max("ltp").alias("high"),
    spark_min("ltp").alias("low"),
    last("ltp").alias("close"),
    spark_sum("buy_volume").alias("buy_volume"),
    spark_sum("sell_volume").alias("sell_volume"),
    (spark_sum("buy_volume") + spark_sum("sell_volume")).alias("total_volume"),
    spark_sum("delta").alias("delta"),
    last("tbq").alias("tbq"),
    last("tsq").alias("tsq")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("instrument"),
    "open", "high", "low", "close",
    "buy_volume", "sell_volume", "total_volume", "delta",
    "tbq", "tsq"
)

# Optional debug: print aggregated metrics
agg_metrics.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .queryName("debug_agg") \
    .start()

output_df = agg_metrics.selectExpr("to_json(struct(*)) AS value")

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", "upstox_orderflow") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/orderflow_to_kafka") \
    .outputMode("append") \
    .start()

query.awaitTermination()

