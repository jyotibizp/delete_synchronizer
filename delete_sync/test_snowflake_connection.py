import os
import logging
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# Configure logging
logging.basicConfig(level=logging.INFO)

# Hardcoded Snowflake connection parameters
SNOWFLAKE_ACCOUNT = "cba42998.east-us-2.azure"
SNOWFLAKE_USER = "IC_IBD_SERVICE_SNOWFLAKE"
SNOWFLAKE_WAREHOUSE = "IC_CRM_WH_XS"
SNOWFLAKE_DATABASE = "IC_CRM_DB"
SNOWFLAKE_SCHEMA = "IC_CRM"
SNOWFLAKE_PRIVATE_KEY_PATH = "certs/rsa_key.p8"

def load_private_key(path: str) -> bytes:
    key_path = Path(path)
    if not key_path.exists():
        raise FileNotFoundError(f"Private key file not found: {path}")

    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def test_snowflake_connection():
    logging.info("Testing Snowflake connection with:")
    logging.info("Account: %s", SNOWFLAKE_ACCOUNT)
    logging.info("User: %s", SNOWFLAKE_USER)
    logging.info("Warehouse: %s", SNOWFLAKE_WAREHOUSE)
    logging.info("Database: %s", SNOWFLAKE_DATABASE)
    logging.info("Schema: %s", SNOWFLAKE_SCHEMA)
    logging.info("Private Key Path: %s", SNOWFLAKE_PRIVATE_KEY_PATH)

    try:
        private_key = load_private_key(SNOWFLAKE_PRIVATE_KEY_PATH)

        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            private_key=private_key
        )

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        logging.info("Connected successfully. Snowflake version: %s", version[0])
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error("Failed to connect to Snowflake: %s", e)

if __name__ == "__main__":
    print("Resolved key path:", os.path.abspath("certs/rsa_key.p8"))
    print("Exists:", os.path.exists("certs/rsa_key.p8"))
    test_snowflake_connection()