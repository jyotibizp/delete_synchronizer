from __future__ import annotations

import logging
from typing import Optional
import snowflake.connector


class CursorStore:
    """Stores replay IDs in Snowflake for persistent, cross-instance storage"""

    def __init__(self, snowflake_connection: snowflake.connector.SnowflakeConnection):
        """
        Initialize cursor store with an active Snowflake connection
        
        Args:
            snowflake_connection: Active Snowflake connection object
        """
        self.connection = snowflake_connection
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        """Create cursor_store table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cursor_store (
            topic VARCHAR(255) PRIMARY KEY,
            replay_id BINARY,
            last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(create_table_sql)
            self.connection.commit()
            logging.info("Ensured cursor_store table exists in Snowflake")
        except Exception as e:
            logging.error("Error creating cursor_store table: %s", e)
            raise
        finally:
            cursor.close()

    def get(self, topic: str) -> Optional[bytes]:
        """
        Get the last replay_id for a topic from Snowflake
        
        Args:
            topic: Topic name (e.g., "/event/Account_Delete__e")
            
        Returns:
            Replay ID as bytes, or None if not found
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT replay_id FROM cursor_store WHERE topic = %s",
                (topic,)
            )
            row = cursor.fetchone()
            
            if row and row[0]:
                replay_id = row[0]
                # Snowflake returns bytearray, but gRPC needs bytes
                if isinstance(replay_id, bytearray):
                    replay_id = bytes(replay_id)
                logging.debug("Retrieved replay_id for topic %s from Snowflake", topic)
                return replay_id
            else:
                logging.debug("No replay_id found for topic %s in Snowflake", topic)
                return None
                
        except Exception as e:
            logging.error("Error retrieving replay_id for topic %s: %s", topic, e)
            return None
        finally:
            cursor.close()

    def set(self, topic: str, replay_id: bytes) -> None:
        """
        Set the replay_id for a topic in Snowflake
        
        Args:
            topic: Topic name (e.g., "/event/Account_Delete__e")
            replay_id: Replay ID as bytes
        """
        cursor = self.connection.cursor()
        try:
            # Use MERGE for upsert behavior (insert or update)
            merge_sql = """
            MERGE INTO cursor_store AS target
            USING (SELECT %s AS topic, %s AS replay_id) AS source
            ON target.topic = source.topic
            WHEN MATCHED THEN 
                UPDATE SET 
                    replay_id = source.replay_id, 
                    last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN 
                INSERT (topic, replay_id, last_updated) 
                VALUES (source.topic, source.replay_id, CURRENT_TIMESTAMP())
            """
            cursor.execute(merge_sql, (topic, replay_id))
            self.connection.commit()
            
            # Convert replay_id to integer for logging
            replay_id_int = int.from_bytes(replay_id, byteorder="big", signed=False)
            logging.info("Saved replay_id %s for topic %s to Snowflake cursor_store", 
                        replay_id_int, topic)
                        
        except Exception as e:
            logging.error("Error saving replay_id for topic %s: %s", topic, e)
            raise
        finally:
            cursor.close()

    def get_all_cursors(self) -> dict[str, bytes]:
        """
        Get all cursors from Snowflake in a single query
        
        Returns:
            Dictionary mapping topic names to replay_ids
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT topic, replay_id FROM cursor_store")
            rows = cursor.fetchall()
            
            # Convert bytearray to bytes (Snowflake returns bytearray, gRPC needs bytes)
            result = {}
            for row in rows:
                if row[1]:
                    replay_id = row[1]
                    if isinstance(replay_id, bytearray):
                        replay_id = bytes(replay_id)
                    result[row[0]] = replay_id
            
            logging.info("Retrieved %d cursors from Snowflake in batch", len(result))
            return result
        except Exception as e:
            logging.error("Error retrieving all cursors: %s", e)
            return {}
        finally:
            cursor.close()
    
    def get_cursors_for_topics(self, topics: list[str]) -> dict[str, bytes]:
        """
        Get cursors for specific topics in a single query (optimized batch fetch)
        
        Args:
            topics: List of topic names to fetch cursors for
            
        Returns:
            Dictionary mapping topic names to replay_ids (only for topics that have cursors)
        """
        if not topics:
            return {}
        
        cursor = self.connection.cursor()
        try:
            # Build parameterized query for multiple topics
            placeholders = ", ".join(["%s"] * len(topics))
            query = f"SELECT topic, replay_id FROM cursor_store WHERE topic IN ({placeholders})"
            
            cursor.execute(query, topics)
            rows = cursor.fetchall()
            
            # Convert bytearray to bytes (Snowflake returns bytearray, gRPC needs bytes)
            result = {}
            for row in rows:
                if row[1]:
                    replay_id = row[1]
                    if isinstance(replay_id, bytearray):
                        replay_id = bytes(replay_id)
                    result[row[0]] = replay_id
            
            logging.info("Retrieved %d cursors for %d topics from Snowflake in single query", 
                        len(result), len(topics))
            return result
        except Exception as e:
            logging.error("Error retrieving cursors for topics: %s", e)
            return {}
        finally:
            cursor.close()
