from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import pandas as pd
import numpy as np

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

rows = session.execute("SELECT * FROM stocks_silver")
data = pd.DataFrame(rows)

if data.empty:
    print("No data found in stocks_silver.")
    cluster.shutdown()
    exit()

data['date'] = data['date'].apply(lambda d: d.datetime.date() if hasattr(d, 'datetime') else d)
data.sort_values(by=['ticker', 'date'], inplace=True)

def compute_indicators(df):
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    return df

gold_df = data.groupby('ticker').apply(compute_indicators).reset_index(drop=True)

session.execute("""
CREATE TABLE IF NOT EXISTS gold_stocks (
    ticker TEXT,
    date DATE,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    ema_20 DECIMAL,
    ema_50 DECIMAL,
    rsi DECIMAL,
    PRIMARY KEY (ticker, date)
) WITH CLUSTERING ORDER BY (date ASC);
""")
session.execute("TRUNCATE gold_stocks")

insert_stmt = session.prepare("""
    INSERT INTO gold_stocks (ticker, date, open, high, low, close, volume, ema_20, ema_50, rsi)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

def to_decimal_safe(value):
    return Decimal(str(round(value, 2))) if pd.notna(value) else None

def insert_batch(rows_batch):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for row in rows_batch:
        cleaned = (
            row['ticker'],
            row['date'],
            to_decimal_safe(row['open']),
            to_decimal_safe(row['high']),
            to_decimal_safe(row['low']),
            to_decimal_safe(row['close']),
            int(row['volume']) if pd.notna(row['volume']) else None,
            to_decimal_safe(row['ema_20']),
            to_decimal_safe(row['ema_50']),
            to_decimal_safe(row['rsi'])
        )
        batch.add(insert_stmt, cleaned)
    try:
        session.execute(batch)
        return len(rows_batch)
    except Exception as e:
        print(f"Batch insert failed: {e}")
        return 0

batch_size = 100
rows_list = gold_df.to_dict('records')
total_inserted = 0

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = []
    for i in range(0, len(rows_list), batch_size):
        batch_rows = rows_list[i:i + batch_size]
        futures.append(executor.submit(insert_batch, batch_rows))

    for i, future in enumerate(as_completed(futures), 1):
        inserted = future.result()
        total_inserted += inserted
        if i % 5 == 0:
            print(f"{total_inserted} records inserted...")

print(f"All data inserted into gold_stocks. Total rows inserted: {total_inserted}")
cluster.shutdown()
