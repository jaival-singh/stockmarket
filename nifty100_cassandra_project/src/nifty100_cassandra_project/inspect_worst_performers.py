from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

select_stmt = """
SELECT ticker, percent_change, start_date, end_date, start_price, end_price 
FROM worst_performing_stocks_by_percent
WHERE dummy_partition = 'all'
"""

rows = session.execute(select_stmt)


for row in rows:
    print(f"Ticker: {row.ticker}, Percent Change: {row.percent_change}%, "
          f"Start Date: {row.start_date}, End Date: {row.end_date}, "
          f"Start Price: {row.start_price}, End Price: {row.end_price}")
