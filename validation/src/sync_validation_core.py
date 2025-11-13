import json
import logging
import pandas as pd
from datetime import datetime
from salesforce import connect_salesforce
from snowflake import connect_snowflake

def get_successfully_synced_objects(cursor):
    """Get list of objects that synced successfully from EXECUTION_TRACKER"""
    try:
        cursor.execute("""
            SELECT DISTINCT OBJECT_NAME
            FROM EXECUTION_TRACKER
            WHERE TYPE = 'SYNC'
              AND STATUS = 'SUCCESS'
              AND INSERTED_DATE >= DATEADD(day, -1, CURRENT_TIMESTAMP())
            ORDER BY OBJECT_NAME
        """)
        results = cursor.fetchall()
        synced_objects = [row[0] for row in results]
        logging.info(f"Found {len(synced_objects)} successfully synced objects: {synced_objects}")
        return synced_objects
    except Exception as e:
        logging.warning(f"Error getting synced objects: {e}. Will validate all objects.")
        return None

def load_entity_mappings(cursor, synced_objects=None):
    """Load entity mappings from ENTITYMAPPING table"""
    try:
        cursor.execute("""
            SELECT ENTITYNAME, MAPPINGTO_SALESFORCE, SNOWFLAKE_TABLENAME,
                   COUNT_SOQL, COUNT_STAGING, COUNT_FINAL
            FROM ENTITYMAPPING
            WHERE COUNT_SOQL IS NOT NULL 
              AND COUNT_STAGING IS NOT NULL 
              AND COUNT_FINAL IS NOT NULL
            ORDER BY ENTITYNAME
        """)
        results = cursor.fetchall()
        mappings = []
        for row in results:
            entity_name = row[0]
            sf_object = row[1]
            
            # Filter by successfully synced objects if provided
            if synced_objects is not None and sf_object not in synced_objects:
                logging.info(f"Skipping {entity_name} - not in successful sync list")
                continue
                
            mappings.append({
                'entity_name': entity_name,
                'sf_object': sf_object,
                'snowflake_table': row[2],
                'count_soql': row[3],
                'count_staging': row[4],
                'count_final': row[5]
            })
        logging.info(f"Loaded {len(mappings)} entity mappings for validation")
        return mappings
    except Exception as e:
        logging.error(f"Error loading entity mappings: {e}")
        return []

def execute_salesforce_query(sf, query, entity_name):
    """Execute custom Salesforce query"""
    try:
        result = sf.query(query)
        return result['records'][0].get('expr0', 0)
    except Exception as e:
        logging.error(f"Error executing Salesforce query for {entity_name}: {e}")
        return -1

def execute_snowflake_query(cursor, query, entity_name):
    """Execute custom Snowflake query"""
    try:
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error executing Snowflake query for {entity_name}: {e}")
        return -1

def get_delete_tracker_stats(cursor):
    cursor.execute("""
        SELECT OBJECT_NAME, STATUS, COUNT(*) as COUNT
        FROM DELETE_TRACKER
        GROUP BY OBJECT_NAME, STATUS
        ORDER BY OBJECT_NAME, STATUS
    """)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(results, columns=columns)

def get_delete_tracker_count(cursor, object_name):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM DELETE_TRACKER WHERE OBJECT_NAME = '{object_name}' AND STATUS = 'open'")
        return cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error querying DELETE_TRACKER for {object_name}: {e}")
        return 0

def get_history_counts(cursor, base_table):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM HISTORY_{base_table}")
        history_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM HISTORY_{base_table} WHERE STATUS = 'DELETED'")
        deleted_count = cursor.fetchone()[0]
        return history_count, deleted_count
    except Exception as e:
        logging.warning(f"Error getting history counts for {base_table}: {e}")
        return 0, 0

def run_sync_validation():
    sf = connect_salesforce()
    conn = connect_snowflake()
    cursor = conn.cursor()
    logging.info("Starting sync validation...")

    # Get successfully synced objects
    synced_objects = get_successfully_synced_objects(cursor)
    
    # Load entity mappings from database
    entity_mappings = load_entity_mappings(cursor, synced_objects)
    if not entity_mappings:
        raise Exception("No entity mappings found for validation")

    delete_stats_df = get_delete_tracker_stats(cursor)
    
    for mapping in entity_mappings:
        entity_name = mapping['entity_name']
        sf_object = mapping['sf_object']
        snowflake_table = mapping['snowflake_table']
        
        logging.info(f"Validating {entity_name} ({sf_object})...")

        # Execute stored queries
        sf_count = execute_salesforce_query(sf, mapping['count_soql'], entity_name)
        snowflake_staging_count = execute_snowflake_query(cursor, mapping['count_staging'], entity_name)
        snowflake_final_count = execute_snowflake_query(cursor, mapping['count_final'], entity_name)
        
        pending_deletes = get_delete_tracker_count(cursor, sf_object)
        history_total, history_deleted = get_history_counts(cursor, snowflake_table)

        staging_match = (sf_count == snowflake_staging_count)
        final_match = (sf_count == snowflake_final_count)

        # Determine status for this object
        if staging_match and final_match and pending_deletes == 0:
            status = "SUCCESS"
            log_message = f"{entity_name}: All counts match. {history_deleted} records in history."
        elif staging_match and final_match and pending_deletes > 0:
            status = "PENDING"
            log_message = f"{entity_name}: Counts match but {pending_deletes} deletes pending."
        else:
            status = "WARNING"
            staging_diff = snowflake_staging_count - sf_count if sf_count >= 0 and snowflake_staging_count >= 0 else None
            final_diff = snowflake_final_count - sf_count if sf_count >= 0 and snowflake_final_count >= 0 else None
            log_message = f"{entity_name}: Count mismatch. Staging diff: {staging_diff}, Final diff: {final_diff}, Pending: {pending_deletes}"

        # Build per-object report
        report_data = {
            'validation_type': 'SYNC_VALIDATION',
            'timestamp': datetime.now().isoformat(),
            'entity_name': entity_name,
            'object': sf_object,
            'counts': {
                'salesforce': sf_count,
                'snowflake_staging': snowflake_staging_count,
                'snowflake_final': snowflake_final_count
            },
            'matches': {
                'staging_match': staging_match,
                'final_match': final_match
            },
            'differences': {
                'staging_diff': snowflake_staging_count - sf_count if sf_count >= 0 and snowflake_staging_count >= 0 else None,
                'final_diff': snowflake_final_count - sf_count if sf_count >= 0 and snowflake_final_count >= 0 else None
            },
            'deletes': {
                'pending_deletes': pending_deletes,
                'history_total': history_total,
                'history_deleted': history_deleted
            },
            'status': status,
            'log_message': log_message
        }

        # Save per-object report to EXECUTION_TRACKER
        try:
            cursor.execute("""
                INSERT INTO EXECUTION_TRACKER (TYPE, STATUS, LOG_MESSAGE, REPORT, OBJECT_NAME)
                VALUES (%s, %s, %s, %s, %s)
            """, ('SYNC_VALIDATION', status, log_message, json.dumps(report_data), sf_object))
            conn.commit()
            logging.info(f"✅ {entity_name}: {status} - {log_message}")
        except Exception as e:
            logging.error(f"Failed to save report for {entity_name}: {e}")

    cursor.close()
    conn.close()
    logging.info(f"Validation completed for {len(entity_mappings)} objects")
    
    return {
        'success': True,
        'message': f'Validation completed for {len(entity_mappings)} objects',
        'total_objects': len(entity_mappings)
    }