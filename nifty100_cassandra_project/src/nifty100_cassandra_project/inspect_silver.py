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


session.set_keyspace("keyspace")

print(" Connected. Fetching rows from stocks_silver...")

rows = session.execute("SELECT * FROM stocks_silver LIMIT 10;")
for row in rows:
    print(row)
