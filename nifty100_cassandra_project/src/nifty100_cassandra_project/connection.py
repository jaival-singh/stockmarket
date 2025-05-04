from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os

# Hardcoded credentials directory
CREDENTIALS_DIR = r"C:\Users\Dell\Documents\big_data\nifty100_cassandra_project\credentials"

# Paths
SECURE_CONNECT_BUNDLE_PATH = os.path.join(CREDENTIALS_DIR, "secure-connect-database.zip")
TOKEN_PATH = os.path.join(CREDENTIALS_DIR, "database-token.json")

# Load database token
with open(TOKEN_PATH) as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

# Connect to AstraDB
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud={'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}, auth_provider=auth_provider)
session = cluster.connect()

# Test connection
def test_connection():
    try:
        row = session.execute("SELECT release_version FROM system.local").one()
        if row:
            print(f"Cassandra Release Version: {row[0]}")
        else:
            print("No result found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_connection()
