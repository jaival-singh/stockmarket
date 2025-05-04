from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
import datetime
from collections import defaultdict

# === Connect to Cassandra ===
CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

# === Create table for Worst Performing Stocks ===
session.execute("""
CREATE TABLE IF NOT EXISTS worst_performing_stocks_by_percent (
    dummy_partition TEXT,
    percent_change DECIMAL,
    ticker TEXT,
    start_date DATE,
    end_date DATE,
    start_price DECIMAL,
    end_price DECIMAL,
    PRIMARY KEY (dummy_partition, percent_change)
) WITH CLUSTERING ORDER BY (percent_change ASC)
""")

# === Get current date and calculate date 3 months ago ===
today = datetime.date.today()
three_months_ago = today - datetime.timedelta(days=90)

# === Fetch all data in last 3 months ===
rows = session.execute(
    "SELECT ticker, date, adj_close FROM stocks_silver WHERE date >= %s ALLOW FILTERING",
    (three_months_ago,)
)

# === Organize data by ticker ===
stock_prices = defaultdict(list)
for row in rows:
    stock_prices[row.ticker].append((row.date, row.adj_close))

# === Compute performance for worst performers ===
worst_performers = []

for ticker, values in stock_prices.items():
    sorted_values = sorted(values, key=lambda x: x[0])
    if len(sorted_values) >= 2:
        start_date, start_price = sorted_values[0]
        end_date, end_price = sorted_values[-1]
        try:
            percent_change = round(((end_price - start_price) / start_price) * 100, 2)
        except ZeroDivisionError:
            percent_change = 0.0

        # Add to worst performers list
        worst_performers.append(('all', percent_change, ticker, start_date, end_date, start_price, end_price))

# === Sort and keep worst 10 only ===
worst_performers.sort(key=lambda x: x[1])  # Sort in ascending order for worst performers
worst_10 = worst_performers[:10]

# === Insert worst 10 into the worst_performing_stocks_by_percent table ===
insert_stmt = session.prepare("""
    INSERT INTO worst_performing_stocks_by_percent (
        dummy_partition, percent_change, ticker, start_date, end_date, start_price, end_price
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
""")

# Insert worst 10
for row in worst_10:
    session.execute(insert_stmt, row)

print("Worst 10 performing stocks inserted into worst_performing_stocks_by_percent table successfully.")
