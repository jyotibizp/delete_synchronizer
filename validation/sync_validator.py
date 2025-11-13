"""
Sync Validator - Local execution wrapper
Validates row counts, delete operations, and history tracking
"""

import sys
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from sync_validation_core import run_sync_validation

def setup_local_env():
    """Setup environment variables for local execution"""
    try:
        load_dotenv()  # Load from .env if available

        # Salesforce Config
        os.environ.setdefault('SF_CLIENT_ID', "3MVG9JEx.BE6yifNk5USP3ASdT_LZ4L6eB_b0FonxbMtt87E8Otty_Au9XXVlYYvrjKAU7CJuO0aLkaCwD7N1")
        os.environ.setdefault('SF_USERNAME', "pradeep.verma@blueowl.com")
        os.environ.setdefault('SF_LOGIN_URL', "https://login.salesforce.com")

        sf_private_key_path = os.path.join(os.path.dirname(__file__), "certs", "private.key")
        if os.path.exists(sf_private_key_path):
            with open(sf_private_key_path, 'r') as f:
                os.environ['SF_PRIVATE_KEY'] = f.read()
        else:
            logging.warning(f"Salesforce private key file not found at {sf_private_key_path}")

        # Snowflake Config
        os.environ.setdefault('SNOWFLAKE_ACCOUNT', "cba42998.east-us-2.azure")
        os.environ.setdefault('SNOWFLAKE_USER', "IC_IBD_SERVICE_SNOWFLAKE")
        os.environ.setdefault('SNOWFLAKE_ROLE', "IC_CRM_DEVELOPER")
        os.environ.setdefault('SNOWFLAKE_AUTHENTICATOR', "snowflake")
        os.environ.setdefault('SNOWFLAKE_WAREHOUSE', "IC_CRM_WH_XS")
        os.environ.setdefault('SNOWFLAKE_DATABASE', "IC_CRM_DB")
        os.environ.setdefault('SNOWFLAKE_SCHEMA', "IC_CRM")

        snowflake_key_path = os.path.join(os.path.dirname(__file__), "certs", "rsa_key.p8")
        if os.path.exists(snowflake_key_path):
            with open(snowflake_key_path, 'r') as f:
                os.environ['SNOWFLAKE_PRIVATE_KEY'] = f.read()
        else:
            logging.warning(f"Snowflake private key file not found at {snowflake_key_path}")

        logging.info("✅ Local environment configured with provided values")
    except Exception as e:
        logging.error(f"Error setting up environment: {e}")
        sys.exit(1)

if __name__ == '__main__':
    try:
        logging.info("🚀 Starting Sync Validator (Local Mode)")
        setup_local_env()
        run_sync_validation()
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        sys.exit(1)