import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from connection import session
import threading

session.set_keyspace('keyspace')

create_table_query = """
CREATE TABLE IF NOT EXISTS stocks (
    date date,
    open float,
    high float,
    low float,
    close float,
    adj_close float,
    volume bigint,
    ticker text,
    PRIMARY KEY (ticker, date)
) WITH CLUSTERING ORDER BY (date ASC);
"""
session.execute(create_table_query)

truncate_query = "TRUNCATE TABLE stocks;"
session.execute(truncate_query)

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

insert_query = """
INSERT INTO stocks (date, open, high, low, close, adj_close, volume, ticker)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
prepared = session.prepare(insert_query)

record_counter = 0
lock = threading.Lock()

def fetch_and_insert(ticker):
    global record_counter
    try:
        data = yf.download(ticker + ".NS", period="10y", interval="1d", auto_adjust=False)
        local_count = 0
        for idx, row in data.iterrows():
            session.execute(prepared, (
                idx.date(),
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                float(row['Adj Close']),
                int(row['Volume']),
                ticker
            ))
            local_count += 1
            with lock:
                record_counter += 1
                if record_counter % 1000 == 0:
                    print(f"{record_counter} records inserted")
        return f"Inserted {local_count} rows for {ticker}"
    except Exception as e:
        return f"Error for {ticker}: {e}"

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(fetch_and_insert, ticker) for ticker in nifty_100_tickers]
    for future in as_completed(futures):
        print(future.result())

print("All data inserted into Cassandra")
