from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import pandas as pd
import json
import os
from datetime import date

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()

session.set_keyspace('keyspace')

session.execute("""
CREATE TABLE IF NOT EXISTS stocks_silver (
    ticker TEXT,
    date DATE,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    adj_close DECIMAL,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
) WITH CLUSTERING ORDER BY (date ASC);
""")
session.execute("TRUNCATE stocks_silver")

rows = session.execute("SELECT * FROM stocks")
df = pd.DataFrame(rows)

df['date'] = df['date'].apply(lambda d: d if isinstance(d, date) else d.date())

def to_decimal(value):
    return Decimal(str(round(value, 2))) if value is not None else None

insert_stmt = session.prepare("""
    INSERT INTO stocks_silver (ticker, date, open, high, low, close, adj_close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
insert_stmt.consistency_level = ConsistencyLevel.QUORUM

def insert_batch(batch_df):
    batch = BatchStatement()
    for _, row in batch_df.iterrows():
        batch.add(insert_stmt, (
            row['ticker'],
            row['date'],
            to_decimal(row['open']),
            to_decimal(row['high']),
            to_decimal(row['low']),
            to_decimal(row['close']),
            to_decimal(row['adj_close']),
            int(row['volume']) if row['volume'] is not None else None
        ))
    session.execute(batch)
    return len(batch_df)

batch_size = 50
total_inserted = 0
futures = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for start in range(0, len(df), batch_size):
        end = start + batch_size
        batch_df = df.iloc[start:end]
        futures.append(executor.submit(insert_batch, batch_df))

    for future in as_completed(futures):
        inserted = future.result()
        total_inserted += inserted
        print(f"Inserted {total_inserted} rows...")

print(f"Data transformation and insertion completed. Total rows inserted: {total_inserted}")
cluster.shutdown()
