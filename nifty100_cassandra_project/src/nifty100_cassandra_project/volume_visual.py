from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
import datetime
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

result = session.execute("SELECT MAX(date) as max_date FROM stocks_silver")
latest_date_row = result.one()
latest_date = latest_date_row.max_date.date() if hasattr(latest_date_row.max_date, "date") else latest_date_row.max_date

start_date = latest_date - datetime.timedelta(days=5)

rows = session.execute(
    "SELECT ticker, date, volume FROM stocks_silver WHERE date >= %s AND date <= %s ALLOW FILTERING",
    (start_date, latest_date)
)

ticker_volume = defaultdict(list)
for row in rows:
    date_val = row.date.date() if hasattr(row.date, "date") else row.date
    ticker_volume[row.ticker].append((date_val, row.volume))

spike_data = []

for ticker, values in ticker_volume.items():
    sorted_values = sorted(values, key=lambda x: x[0])
    df = pd.DataFrame([(d, v) for d, v in sorted_values], columns=["date", "volume"]).sort_values("date")

    if latest_date not in df["date"].values or len(df) < 2:
        continue

    latest_volume = df[df["date"] == latest_date]["volume"].values[0]
    avg_prev_volume = df[df["date"] < latest_date]["volume"].mean()

    if avg_prev_volume > 0:
        spike_ratio = latest_volume / avg_prev_volume
        spike_data.append((ticker, spike_ratio, latest_volume, avg_prev_volume))

spike_data.sort(key=lambda x: x[1], reverse=True)

top_10 = spike_data[:10]
tickers = [x[0] for x in top_10]
ratios = [x[1] for x in top_10]

plt.figure(figsize=(12, 6))
bars = plt.bar(tickers, ratios, color='skyblue')
plt.xlabel("Ticker")
plt.ylabel("Volume Spike (Latest / Avg Previous 5 Days)")
plt.title(f"Top 10 Stocks by Volume Spike on {latest_date}")
plt.xticks(rotation=45)
plt.tight_layout()

for bar, ratio in zip(bars, ratios):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{ratio:.2f}", ha='center', va='bottom')

plt.show()
