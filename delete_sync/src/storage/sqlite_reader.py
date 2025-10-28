from __future__ import annotations

import json
import sqlite3
from typing import List, Dict


def read_events_from_db(db_path: str) -> List[Dict]:
    """
    Read all events from a SQLite database file
    
    Returns:
        List of event dictionaries with parsed JSON payloads
    """
    events = []
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT id, topic, payload
            FROM events
            ORDER BY id
            """
        )
        
        for row in cursor:        
            topic = row[1]
            payload_json = json.loads(row[2])  # Parse the JSON string

            object_name = topic.replace("/event/", "").replace("_Delete__e", "")
            record_id = payload_json.get(f"{object_name}_Id__c")
            deleted_by = payload_json.get("Deleted_By__c")

            event = {
                "object_name": object_name,
                "record_id": record_id,
                "deleted_by": deleted_by
            }
            events.append(event)
    return events

