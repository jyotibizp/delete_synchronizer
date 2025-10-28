import datetime
import logging
import azure.functions as func

from src.config.settings import get_settings
from src.storage.blob_reader import list_event_files, download_file
from src.storage.sqlite_reader import read_events_from_db
from src.snowflake.connector import SnowflakeConnector


def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info("Snowflake pusher function started at %s", utc_timestamp)

    settings = get_settings()
    
    # List all event database files from blob storage
    logging.info("Listing event files from storage...")
    event_files = list_event_files(
        settings.azure_storage_connection_string,
        settings.azure_blob_container,
        settings.environment
    )
    
    if not event_files:
        logging.info("No event files found to process")
        return
    
    logging.info("Found %d event files to process", len(event_files))
    logging.info(
    "Initializing SnowflakeConnector with account=%s, user=%s, warehouse=%s, "
    "database=%s, schema=%s, table=%s, private_key_path=%s",
    settings.snowflake_account,
    settings.snowflake_user,
    settings.snowflake_warehouse,
    settings.snowflake_database,
    settings.snowflake_schema,
    settings.snowflake_table,
    settings.snowflake_private_key_path,)

    # Initialize Snowflake connector
    snowflake_conn = SnowflakeConnector(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        table=settings.snowflake_table,
        private_key_path=settings.snowflake_private_key_path,
    )
    
    total_events = 0
    processed_files = 0
    
    try:
        snowflake_conn.connect()
        snowflake_conn.ensure_table_exists()
        
        # Process each event file
        for file_info in event_files:
            file_path = file_info["path"]
            logging.info("Processing file: %s", file_path)
            
            try:
                # Download file from blob storage to temp location
                local_path = download_file(
                    settings.azure_storage_connection_string,
                    settings.azure_blob_container,
                    file_path,
                    settings.environment
                )

                logging.info("Dowloaded file %s", local_path)
                
                # Read events from SQLite database
                events = read_events_from_db(local_path)
                logging.info("Read %d events from %s", len(events), file_path)
                
                if events:
                    # Insert events into Snowflake
                    inserted = snowflake_conn.insert_events(events)
                    total_events += inserted
                    logging.info("Inserted %d events from %s into Snowflake", inserted, file_path)
                
                processed_files += 1
                
            except Exception as e:
                logging.error("Error processing file %s: %s", file_path, e)
                continue
        
        logging.info(
            "Snowflake pusher completed: processed %d files, inserted %d total events",
            processed_files,
            total_events
        )
        
    finally:
        snowflake_conn.close()
    
    logging.info("Snowflake pusher function completed at %s", datetime.datetime.utcnow().isoformat())

