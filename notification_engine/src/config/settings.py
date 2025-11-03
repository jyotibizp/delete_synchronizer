import os

class Settings:
    """Configuration settings from environment variables"""
    
    # Snowflake settings
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'IC_CRM_DB')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'IC_CRM')
    
    # Email settings
    SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
    SENDER_EMAIL = os.getenv('SENDER_EMAIL')
    SUBSCRIBER_EMAILS = os.getenv('SUBSCRIBER_EMAILS', '').split(',')
    
    @classmethod
    def validate(cls):
        """Validate required settings are present"""
        required = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SENDGRID_API_KEY',
            'SENDER_EMAIL'
        ]
        
        missing = [field for field in required if not getattr(cls, field)]
        
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")
        
        if not cls.SUBSCRIBER_EMAILS or cls.SUBSCRIBER_EMAILS == ['']:
            raise ValueError("SUBSCRIBER_EMAILS must contain at least one email")

