from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
import datetime
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

query_top = "SELECT ticker, start_date, end_date FROM top_performing_stocks_by_percent;"
top_performers = session.execute(query_top)

for row in top_performers:
    ticker = row.ticker
    start_date = row.start_date
    end_date = row.end_date

    query_prices = """
        SELECT date, adj_close FROM stocks_silver
        WHERE ticker = %s AND date >= %s AND date <= %s
        ALLOW FILTERING;
    """
    stock_data = session.execute(query_prices, (ticker, start_date, end_date))

    dates = []
    prices = []

    for data in stock_data:
        try:
            date_obj = data.date if isinstance(data.date, datetime.date) else data.date.date()
            price = float(data.adj_close)
            dates.append(date_obj)
            prices.append(price)
        except Exception as e:
            pass

    if dates and prices:
        plt.figure(figsize=(10, 5))
        plt.plot(dates, prices, marker='o', linestyle='-', label=ticker)
        plt.xlabel('Date')
        plt.ylabel('Adjusted Close Price')
        plt.title(f"{ticker} Stock Price Movement")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        plt.legend()
        plt.show()
    else:
        pass
