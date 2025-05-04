import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
from connection import session  

session.set_keyspace('keyspace')  

select_count_query = "SELECT COUNT(*) FROM stocks"
rows = session.execute(select_count_query)

for row in rows:
    print(f"Total Rows in stocks table: {row.count}")

select_top_query = "SELECT * FROM stocks LIMIT 10"
top_rows = session.execute(select_top_query)

top_rows_list = list(top_rows)
if top_rows_list:
    columns = top_rows_list[0]._fields
    print(f"Total Columns in stocks table: {len(columns)}")

    print("\nTop 10 Records:")
    for record in top_rows_list:
        print(record)
else:
    print("No records found.")

spark = SparkSession.builder \
    .appName("StocksTickerCount") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  
select_all_query = "SELECT * FROM stocks"
all_rows = session.execute(select_all_query)

all_data_list = [row._asdict() for row in all_rows]

if not all_data_list:
    print("No data found in the stocks table.")
else:
    
    df = spark.createDataFrame(all_data_list)

    
    print("\nNULL values count per column:")

    from pyspark.sql import functions as F

    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns
    ])

    null_counts.show(truncate=False)

    
    ticker_df = df.filter(F.col("ticker").isNotNull())

    ticker_count_df = ticker_df.groupBy("ticker").count()

    print("\nNumber of records per ticker (non-null tickers only):")
    ticker_count_df.orderBy("count", ascending=False).show(100, truncate=False)


spark.stop()
