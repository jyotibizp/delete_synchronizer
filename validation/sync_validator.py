"""
Sync Validator - Local execution wrapper
Validates row counts, delete operations, and history tracking
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from sync_validation_core import run_sync_validation


# For local execution, set environment variables from config.py
def setup_local_env():
    """Setup environment variables for local execution"""
    try:
        from src.config import settings
        os.environ['SF_CLIENT_ID'] = settings.SF_CLIENT_ID
        os.environ['SF_USERNAME'] = settings.SF_USERNAME
        os.environ['SF_LOGIN_URL'] = settings.SF_LOGIN_URL
        
        # Load private key from file and set as environment variable
        with open(settings.SF_PRIVATE_KEY_FILE, 'r') as f:
            os.environ['SF_PRIVATE_KEY'] = f.read()
        
        # For local development, use browser authentication for Snowflake
        # The core module will need to be updated to support this
        os.environ['SNOWFLAKE_ACCOUNT'] = "NP91221-IC_IBD_CRM"
        os.environ['SNOWFLAKE_USER'] = "rajani.gutha@blueowl.com"
        os.environ['SNOWFLAKE_AUTHENTICATOR'] = 'externalbrowser'
        os.environ['SNOWFLAKE_ROLE'] = "IC_CRM_DEVELOPER"
        os.environ['SNOWFLAKE_WAREHOUSE'] = "IC_CRM_WH_XS"
        os.environ['SNOWFLAKE_DATABASE'] = "IC_CRM_DB"
        os.environ['SNOWFLAKE_SCHEMA'] = "IC_CRM"
        
        print("✅ Local environment configured")
    except ImportError:
        print("⚠️  config.py not found. Make sure environment variables are set manually.")
    except Exception as e:
        print(f"⚠️  Error loading config: {e}")


if __name__ == '__main__':
    try:
        print("🚀 Starting Sync Validator (Local Mode)")
        setup_local_env()
        run_sync_validation()
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()

