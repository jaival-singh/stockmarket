import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lag, when, avg, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql.functions import pandas_udf, PandasUDFType
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("ASTRA_DB_CLIENT_ID")
secret = os.getenv("ASTRA_DB_SECRET")
bundle_path = os.getenv("ASTRA_DB_BUNDLE_PATH")

print("Checking environment variables...")
print("Client ID:", client_id)
print("Secret present:", bool(secret))
print("Bundle path:", bundle_path)

if not all([client_id, secret, bundle_path]):
    raise ValueError("Missing required environment variables for Cassandra connection.")

spark = SparkSession.builder \
    .appName("ComputeTechnicalIndicators") \
    .config("spark.cassandra.connection.config.cloud.path", bundle_path) \
    .config("spark.cassandra.auth.username", client_id) \
    .config("spark.cassandra.auth.password", secret) \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .getOrCreate()

print("Spark session started.")

try:
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="stocks_silver", keyspace="keyspace") \
        .load()
    print("Schema loaded:")
    df.printSchema()
    print(f"Number of rows loaded: {df.count()}")
except Exception as e:
    print("Error reading from Cassandra:", e)
    spark.stop()
    exit()

if df.rdd.isEmpty():
    print("No data found in stocks_silver.")
    spark.stop()
    exit()

df = df.withColumn("date", to_date(col("date")))
windowSpec = Window.partitionBy("ticker").orderBy("date")
df = df.withColumn("delta", col("close") - lag("close", 1).over(windowSpec))
df = df.withColumn("gain", when(col("delta") > 0, col("delta")).otherwise(0))
df = df.withColumn("loss", when(col("delta") < 0, -col("delta")).otherwise(0))
avg_gain = avg("gain").over(windowSpec.rowsBetween(-13, 0))
avg_loss = avg("loss").over(windowSpec.rowsBetween(-13, 0))
df = df.withColumn("avg_gain", avg_gain)
df = df.withColumn("avg_loss", avg_loss)
df = df.withColumn("rs", col("avg_gain") / col("avg_loss"))
df = df.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
df = df.select("ticker", "date", "close", "rsi").dropna()

print("Data after RSI computation (showing 5 rows):")
df.show(5)

@pandas_udf("ticker string, date date, close double, rsi double, ema_20 double, ema_50 double", PandasUDFType.GROUPED_MAP)
def compute_emas(pdf):
    pdf = pdf.sort_values("date")
    pdf["ema_20"] = pdf["close"].ewm(span=20, adjust=False).mean()
    pdf["ema_50"] = pdf["close"].ewm(span=50, adjust=False).mean()
    return pdf[["ticker", "date", "close", "rsi", "ema_20", "ema_50"]]

print("Applying EMA calculations...")
gold_df = df.groupBy("ticker").apply(compute_emas)

print("Data after EMA computation (showing 5 rows):")
gold_df.show(5)

gold_df = gold_df.withColumn("close", spark_round(col("close"), 4)) \
                 .withColumn("ema_20", spark_round(col("ema_20"), 4)) \
                 .withColumn("ema_50", spark_round(col("ema_50"), 4)) \
                 .withColumn("rsi", spark_round(col("rsi"), 4))

try:
    gold_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="gold_stocks", keyspace="keyspace") \
        .mode("append") \
        .save()
    print("Technical indicators computed and data inserted into gold_stocks.")
except Exception as e:
    print("Error writing to Cassandra:", e)

spark.stop()
print("Spark session stopped.")
