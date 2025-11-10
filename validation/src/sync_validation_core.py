"""
Core sync validation logic for Azure Function
Validates row counts, delete operations, and history tracking
"""

import json
import os
import logging
import pandas as pd
import snowflake.connector
from simple_salesforce import Salesforce
from cryptography.hazmat.primitives import serialization
from jwt import encode as jwt_encode
import time
import requests
from datetime import datetime

# Add parent directory to path
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data_dict import SF_OBJECT_TO_SNOWFLAKE_TABLE


# ==================== Authentication ====================

def load_private_key_from_env():
    """Load private key from environment variable"""
    private_key_content = os.environ.get('SF_PRIVATE_KEY')
    if not private_key_content:
        raise Exception("SF_PRIVATE_KEY environment variable not set")
    
    # Handle both base64 encoded and direct PEM format
    if not private_key_content.startswith('-----BEGIN'):
        import base64
        private_key_content = base64.b64decode(private_key_content).decode('utf-8')
    
    return serialization.load_pem_private_key(
        private_key_content.encode('utf-8'),
        password=None
    )


def create_jwt_assertion(client_id, username, login_url, private_key):
    """Create JWT assertion for Salesforce"""
    issued_at = int(time.time())
    expiration = issued_at + 300
    payload = {
        'iss': client_id,
        'sub': username,
        'aud': str(login_url),
        'exp': expiration,
        'iat': issued_at
    }
    return jwt_encode(payload, private_key, algorithm='RS256')


def get_salesforce_access_token(jwt_token, login_url):
    """Get Salesforce access token"""
    token_url = f'{login_url}/services/oauth2/token'
    response = requests.post(token_url, data={
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token
    })
    return response.json()


def connect_salesforce():
    """Connect to Salesforce using JWT authentication"""
    logging.info("Connecting to Salesforce...")
    
    # Get credentials from environment
    client_id = os.environ.get('SF_CLIENT_ID')
    username = os.environ.get('SF_USERNAME')
    login_url = os.environ.get('SF_LOGIN_URL', 'https://login.salesforce.com')
    
    if not client_id or not username:
        raise Exception("SF_CLIENT_ID and SF_USERNAME environment variables must be set")
    
    private_key = load_private_key_from_env()
    jwt_token = create_jwt_assertion(client_id, username, login_url, private_key)
    response_data = get_salesforce_access_token(jwt_token, login_url)
    
    if 'access_token' not in response_data:
        raise Exception(f"Failed to retrieve Salesforce access token: {response_data}")
    
    logging.info("Connected to Salesforce")
    return Salesforce(
        instance_url=response_data['instance_url'],
        session_id=response_data['access_token']
    )


def connect_snowflake():
    """Connect to Snowflake using credentials from environment"""
    logging.info("Connecting to Snowflake...")
    
    # Get credentials from environment
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    authenticator = os.environ.get('SNOWFLAKE_AUTHENTICATOR', 'snowflake')
    role = os.environ.get('SNOWFLAKE_ROLE', 'IC_CRM_DEVELOPER')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE', 'IC_CRM_WH_XS')
    database = os.environ.get('SNOWFLAKE_DATABASE', 'IC_CRM_DB')
    schema = os.environ.get('SNOWFLAKE_SCHEMA', 'IC_CRM')
    
    if not account or not user:
        raise Exception("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set")
    
    # Build connection params
    conn_params = {
        'account': account,
        'user': user,
        'role': role,
        'warehouse': warehouse,
        'database': database,
        'schema': schema
    }
    
    # Add authentication
    if authenticator == 'externalbrowser':
        conn_params['authenticator'] = 'externalbrowser'
    elif password:
        conn_params['password'] = password
    else:
        raise Exception("Either SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR=externalbrowser must be set")
    
    conn = snowflake.connector.connect(**conn_params)
    
    logging.info("Connected to Snowflake")
    return conn


# ==================== Validation Functions ====================

def get_salesforce_count(sf, sf_object):
    """Get row count from Salesforce object"""
    try:
        result = sf.query(f"SELECT COUNT(Id) FROM {sf_object}")
        return result['records'][0].get('expr0', 0)
    except Exception as e:
        logging.error(f"Error querying Salesforce {sf_object}: {e}")
        return -1


def get_snowflake_count(cursor, table_name):
    """Get row count from Snowflake table"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error querying Snowflake {table_name}: {e}")
        return -1


def get_delete_tracker_stats(cursor):
    """Get delete tracker statistics by status"""
    cursor.execute("""
        SELECT 
            OBJECT_NAME,
            STATUS,
            COUNT(*) as COUNT
        FROM DELETE_TRACKER
        GROUP BY OBJECT_NAME, STATUS
        ORDER BY OBJECT_NAME, STATUS
    """)
    
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=columns)


def get_delete_tracker_count(cursor, object_name):
    """Get count of pending deletes for specific object"""
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM DELETE_TRACKER WHERE OBJECT_NAME = '{object_name}' AND STATUS = 'open'"
        )
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error querying DELETE_TRACKER for {object_name}: {e}")
        return 0


def get_history_counts(cursor, base_table):
    """Get history table counts"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM HISTORY_{base_table}")
        history_count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM HISTORY_{base_table} WHERE STATUS = 'DELETED'")
        deleted_count = cursor.fetchone()[0]
        
        return history_count, deleted_count
    except Exception as e:
        logging.warning(f"Error getting history counts for {base_table}: {e}")
        return 0, 0


# ==================== Main Validation ====================

def run_sync_validation():
    """Main validation function - validates everything in one run"""
    
    # Connect to both systems
    sf = connect_salesforce()
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    logging.info("Starting sync validation...")
    
    # Get DELETE_TRACKER summary
    delete_stats_df = get_delete_tracker_stats(cursor)
    
    # Validation results for each object
    validation_results = []
    
    for mapping in SF_OBJECT_TO_SNOWFLAKE_TABLE:
        sf_object = mapping['sf_object']
        sf_table = mapping['snowflake_table']
        sf_final_table = mapping['snowflake_final_table']
        
        logging.info(f"Validating {sf_object}...")
        
        # Get counts
        sf_count = get_salesforce_count(sf, sf_object)
        snowflake_base_count = get_snowflake_count(cursor, sf_table)
        snowflake_final_count = get_snowflake_count(cursor, sf_final_table)
        pending_deletes = get_delete_tracker_count(cursor, sf_object)
        history_total, history_deleted = get_history_counts(cursor, sf_table)
        
        # Calculate match status
        base_match = (sf_count == snowflake_base_count)
        final_match = (sf_count == snowflake_final_count)
        
        validation_results.append({
            'object': sf_object,
            'salesforce_count': sf_count,
            'snowflake_base_count': snowflake_base_count,
            'snowflake_final_count': snowflake_final_count,
            'base_match': base_match,
            'final_match': final_match,
            'base_diff': snowflake_base_count - sf_count if sf_count >= 0 and snowflake_base_count >= 0 else None,
            'final_diff': snowflake_final_count - sf_count if sf_count >= 0 and snowflake_final_count >= 0 else None,
            'pending_deletes': pending_deletes,
            'history_total': history_total,
            'history_deleted': history_deleted
        })
        
        status = 'MATCH' if base_match and final_match else 'MISMATCH'
        logging.info(f"{status}: {sf_object} - SF={sf_count}, Base={snowflake_base_count}, Final={snowflake_final_count}")
    
    # Calculate summary
    total_objects = len(validation_results)
    base_matches = sum(1 for r in validation_results if r['base_match'])
    final_matches = sum(1 for r in validation_results if r['final_match'])
    all_match = base_matches == total_objects and final_matches == total_objects
    
    total_pending = sum(r['pending_deletes'] for r in validation_results)
    total_deleted = sum(r['history_deleted'] for r in validation_results)
    
    # Determine overall status
    if all_match and total_pending == 0:
        status = "SUCCESS"
        log_message = f"All {total_objects} objects validated. Counts match. {total_deleted} records in history."
    elif all_match and total_pending > 0:
        status = "PENDING"
        log_message = f"Counts match but {total_pending} deletes pending across {total_objects} objects."
    else:
        mismatches = sum(1 for r in validation_results if not r['base_match'] or not r['final_match'])
        status = "WARNING"
        log_message = f"{mismatches} objects with count mismatches. {total_pending} deletes pending."
    
    # Prepare comprehensive report
    report_data = {
        'validation_type': 'SYNC_VALIDATION',
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'status': status,
            'total_objects': total_objects,
            'base_matches': base_matches,
            'final_matches': final_matches,
            'all_counts_match': all_match,
            'total_pending_deletes': total_pending,
            'total_deleted_in_history': total_deleted,
            'log_message': log_message
        },
        'delete_tracker_summary': delete_stats_df.to_dict(orient='records') if not delete_stats_df.empty else [],
        'validation_results': validation_results
    }
    
    # Save to EXECUTION_TRACKER
    try:
        cursor.execute("""
            INSERT INTO EXECUTION_TRACKER (TYPE, STATUS, LOG_MESSAGE, REPORT)
            VALUES (%s, %s, %s, %s)
        """, ('SYNC_VALIDATION', status, log_message, json.dumps(report_data)))
        conn.commit()
        logging.info(f"Results saved to EXECUTION_TRACKER (Status: {status})")
    except Exception as e:
        logging.error(f"Failed to save to EXECUTION_TRACKER: {e}")
    
    # Close connections
    cursor.close()
    conn.close()
    
    logging.info(f"Validation completed: {status}")
    
    return report_data

