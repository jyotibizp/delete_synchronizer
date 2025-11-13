import os
import logging
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def connect_snowflake():
    """Connect to Snowflake using key-pair authentication"""
    logging.info("Connecting to Snowflake using private key...")

    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    role = os.environ.get('SNOWFLAKE_ROLE', 'IC_CRM_DEVELOPER')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE', 'IC_CRM_WH_XS')
    database = os.environ.get('SNOWFLAKE_DATABASE', 'IC_CRM_DB')
    schema = os.environ.get('SNOWFLAKE_SCHEMA', 'IC_CRM')
    authenticator = os.environ.get('SNOWFLAKE_AUTHENTICATOR', 'snowflake')
    private_key = os.environ.get('SNOWFLAKE_PRIVATE_KEY')

    if not account or not user:
        raise Exception("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set")
    if not private_key:
        raise Exception("SNOWFLAKE_PRIVATE_KEY must be set in environment")

    try:
        private_key_obj = serialization.load_pem_private_key(private_key.encode(), password=None)
    except Exception as e:
        raise Exception(f"Failed to load private key: {e}")

    conn_params = {
        'account': account,
        'user': user,
        'role': role,
        'warehouse': warehouse,
        'database': database,
        'schema': schema,
        'authenticator': authenticator,
        'private_key': private_key_obj
    }

    conn = snowflake.connector.connect(**conn_params)
    logging.info("✅ Connected to Snowflake using key-pair authentication")
    return conn