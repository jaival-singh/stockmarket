import yfinance as yf
from decimal import Decimal
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel
import json
import os
from datetime import datetime

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

nifty_100_tickers = [
    "ADANIENT", "ADANIENSOL", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER",
    "ATGL", "AMBUJACEM", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BANKBARODA",
    "BEL", "BHEL", "BPCL", "BOSCHLTD", "BRITANNIA",
    "CANBK", "CHOLAFIN", "CIPLA", "COALINDIA", "DABUR",
    "DIVISLAB", "DLF", "EICHERMOT", "GAIL", "GODREJCP",
    "GRASIM", "HAVELLS", "HCLTECH", "HDFC", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HAL", "HINDUNILVR",
    "ICICIBANK", "ICICIGI", "ICICIPRULI", "INDHOTEL", "IOC",
    "INDUSINDBK", "INFY", "ITC", "JINDALSTEL", "JIOFIN",
    "JSWENERGY", "JSWSTEEL", "KOTAKBANK", "LT", "LTIM",
    "M&M", "MARUTI", "MOTHERSON", "NESTLEIND", "NHPC",
    "NTPC", "ONGC", "PFC", "PIDILITIND", "POWERGRID",
    "RECLTD", "RELIANCE", "SBILIFE", "SHREECEM", "SHRIRAMFIN",
    "SIEMENS", "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATAPOWER",
    "TATASTEEL", "TCS", "TECHM", "TITAN", "TORNTPHARM",
    "TRENT", "TVSMOTOR", "ULTRACEMCO", "UNIONBANK", "MCDOWELL-N",
    "VBL", "VEDL", "WIPRO", "ZOMATO"
]

def to_decimal(value):
    return Decimal(str(round(value, 2))) if value is not None else None

bronze_insert_stmt = session.prepare("""
INSERT INTO stocks (date, open, high, low, close, adj_close, volume, ticker)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

today_str = datetime.today().strftime('%Y-%m-%d')
print(f"Fetching and updating data for {today_str}...")

for ticker in nifty_100_tickers:
    try:
        print(f"Fetching {ticker}...")
        data = yf.download(ticker + ".NS", start=today_str, end=today_str, interval="1d", auto_adjust=False)
        if data.empty:
            print(f"No data available for {ticker} today.")
            continue

        row = data.iloc[0]
        session.execute(bronze_insert_stmt, (
            row.name.date(),
            float(row['Open']),
            float(row['High']),
            float(row['Low']),
            float(row['Close']),
            float(row['Adj Close']),
            int(row['Volume']),
            ticker
        ))
        print(f"{ticker} updated in bronze table.")
    except Exception as e:
        print(f"Error updating {ticker}: {e}")

print("Starting silver layer update...")
silver_insert_stmt = session.prepare("""
INSERT INTO stocks_silver (ticker, date, open, high, low, close, adj_close, volume)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

today_date = datetime.today().date()
today_rows = session.execute("SELECT * FROM stocks WHERE date = %s ALLOW FILTERING", [today_date])

batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
batch_size = 50
count = 0
total_inserted = 0

def flush_batch():
    global total_inserted
    try:
        session.execute(batch)
        total_inserted += len(batch)
        print(f"Inserted {total_inserted} rows into silver...")
    except Exception as e:
        print(f"Batch insert failed at {total_inserted}: {e}")
    finally:
        batch.clear()

for row in today_rows:
    cleaned = (
        row.ticker,
        row.date,
        to_decimal(row.open),
        to_decimal(row.high),
        to_decimal(row.low),
        to_decimal(row.close),
        to_decimal(row.adj_close),
        row.volume
    )
    batch.add(silver_insert_stmt, cleaned)
    count += 1

    if count % batch_size == 0:
        flush_batch()

if batch:
    flush_batch()

print(f"Silver layer update completed. Total rows inserted: {total_inserted}")
cluster.shutdown()
