from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
import pandas as pd

CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

with open(TOKEN_PATH) as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('keyspace')

print("Fetching top 10 records from gold_stocks table...")
query_result = session.execute("SELECT * FROM gold_stocks LIMIT 10")
top10_df = pd.DataFrame(query_result)

if not top10_df.empty:
    top10_df['date'] = top10_df['date'].apply(lambda d: d.datetime.date() if hasattr(d, 'datetime') else d)
    print(top10_df)
else:
    print("No records found in gold_stocks.")

cluster.shutdown()
