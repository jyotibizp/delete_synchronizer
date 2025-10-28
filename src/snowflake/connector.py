from __future__ import annotations

import json
import logging
from typing import Dict, List
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.connector import DictCursor


class SnowflakeConnector:
    """Snowflake connector for inserting delete events
    
    Uses RSA key authentication (recommended for production)
    """

    def __init__(
        self,
        account: str,
        user: str,
        warehouse: str,
        database: str,
        schema: str,
        table: str,
        private_key_path: str,
    ):
        self.account = account
        self.user = user
        self.private_key_path = private_key_path
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.table = table
        self.connection = None

    def _load_private_key(self) -> bytes:
        """Load and parse RSA private key from file"""
        if not self.private_key_path:
            raise ValueError("Private key path not provided")
        
        key_path = Path(self.private_key_path)
        if not key_path.exists():
            raise FileNotFoundError(f"Private key file not found: {self.private_key_path}")
        
        with open(key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        # Serialize to DER format (required by snowflake-connector-python)
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return private_key_bytes

    def connect(self):
        """Establish connection to Snowflake using RSA key authentication"""
        logging.info("Connecting to Snowflake using RSA key authentication")
        
        conn_params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "private_key": self._load_private_key(),
        }
        
        self.connection = snowflake.connector.connect(**conn_params)
        logging.info("Connected to Snowflake: %s.%s.%s", self.database, self.schema, self.table)

    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logging.info("Closed Snowflake connection")

    def ensure_table_exists(self):
        """Create the delete tracker table if it doesn't exist"""
        if not self.connection:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            id INTEGER AUTOINCREMENT,
            object_name VARCHAR(255) NOT NULL,
            record_id VARCHAR(255),
            deleted_by VARCHAR(255),
            delete_tracked_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            status VARCHAR(255),
            PRIMARY KEY (id)
        )
        """

        cursor = self.connection.cursor()
        try:
            cursor.execute(create_table_sql)
            logging.info("Ensured table exists: %s", self.table)
        finally:
            cursor.close()

    def insert_events(self, events: List[Dict]) -> int:
        """
        Insert delete events into Snowflake table.

        Returns:
            Number of events inserted
        """
        if not self.connection:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        if not events:
            return 0

        insert_sql = f"""
        INSERT INTO {self.table} (
            object_name, record_id, deleted_by, status
        )
        VALUES (
            %(object_name)s, %(record_id)s, %(deleted_by)s, %(status)s
        )
        """

        cursor = self.connection.cursor()
        inserted = 0

        try:
            for event in events:
                params = {
                    "object_name": event.get("object_name"),
                    "record_id": event.get("record_id"),
                    "deleted_by": event.get("deleted_by"),
                    "status": event.get("status", "open"),
                }

                try:
                    cursor.execute(insert_sql, params)
                    inserted += 1
                except Exception as e:
                    logging.error("Error inserting event: %s", e)
                    continue

            self.connection.commit()
            logging.info("Inserted %d events into Snowflake", inserted)

        finally:
            cursor.close()

        return inserted

