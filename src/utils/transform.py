"""Event transformation utilities for converting Salesforce events to Snowflake format"""

from typing import Dict, List
import logging


def transform_for_snowflake(events: List[Dict]) -> List[Dict]:
    """
    Convert Salesforce Pub/Sub events to Snowflake insert format
    
    Args:
        events: List of events from Salesforce Pub/Sub API with structure:
            {
                "topic": str,
                "replay_id": bytes,
                "event_id": str,
                "payload": dict (decoded Avro payload)
            }
    
    Returns:
        List of events formatted for Snowflake insertion:
            {
                "object_name": str,
                "record_id": str,
                "deleted_by": str,
                "status": str
            }
    """
    transformed = []
    
    for event in events:
        try:
            topic = event.get("topic", "")
            payload = event.get("payload", {})
            
            # Extract object name from topic
            # e.g., "/event/Account_Delete__e" -> "Account"
            object_name = topic.replace("/event/", "").replace("_Delete__e", "")
            
            # Build the field name dynamically
            # e.g., "Account" -> "Account_Id__c"
            record_id_field = f"{object_name}_Id__c"
            record_id = payload.get(record_id_field)
            
            # Standard delete event fields
            deleted_by = payload.get("Deleted_By__c")
            
            transformed.append({
                "object_name": object_name,
                "record_id": record_id,
                "deleted_by": deleted_by,
                "status": "open"
            })
            
        except Exception as e:
            logging.error("Error transforming event: %s", e)
            continue
    
    return transformed

