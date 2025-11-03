import snowflake.connector
import logging
from datetime import date
from ..config.settings import Settings

class SnowflakeConnector:
    """Handles Snowflake database connections and queries"""
    
    def __init__(self):
        Settings.validate()
        self.connection = None
    
    def _get_connection(self):
        """Create and return Snowflake connection"""
        if not self.connection:
            self.connection = snowflake.connector.connect(
                account=Settings.SNOWFLAKE_ACCOUNT,
                user=Settings.SNOWFLAKE_USER,
                password=Settings.SNOWFLAKE_PASSWORD,
                warehouse=Settings.SNOWFLAKE_WAREHOUSE,
                database=Settings.SNOWFLAKE_DATABASE,
                schema=Settings.SNOWFLAKE_SCHEMA
            )
        return self.connection
    
    def fetch_daily_executions(self, target_date: date) -> list:
        """
        Fetch all execution records for a specific date.
        
        Args:
            target_date: The date to fetch records for
            
        Returns:
            List of execution records with columns: ID, TYPE, STATUS, LOG_MESSAGE, REPORT, INSERTED_DATE
        """
        query = """
        SELECT 
            ID,
            TYPE,
            STATUS,
            LOG_MESSAGE,
            REPORT,
            INSERTED_DATE
        FROM EXECUTION_TRACKER
        WHERE DATE(INSERTED_DATE) = %s
        ORDER BY INSERTED_DATE
        """
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            logging.info(f'Executing query for date: {target_date}')
            cursor.execute(query, (target_date,))
            
            # Fetch all results
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor:
                record = dict(zip(columns, row))
                results.append(record)
            
            cursor.close()
            return results
            
        except Exception as e:
            logging.error(f'Error fetching executions from Snowflake: {str(e)}')
            raise
    
    def close(self):
        """Close the database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

