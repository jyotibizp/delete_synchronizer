import os

class Settings:
    """Configuration settings from environment variables"""
    
    # Snowflake settings
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'IC_CRM_DB')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'IC_CRM')
    
    # Email settings
    SMTP_HOST = os.getenv('SMTP_HOST')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    SMTP_USERNAME = os.getenv('SMTP_USERNAME')
    SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')
    SMTP_USE_TLS = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
    SENDER_EMAIL = os.getenv('SENDER_EMAIL')
    SUBSCRIBER_EMAILS = os.getenv('SUBSCRIBER_EMAILS', '').split(',')
    
    @classmethod
    def validate(cls):
        """Validate required settings are present"""
        # Required settings
        required = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PRIVATE_KEY_PATH',
            'SMTP_HOST',
            'SMTP_USERNAME',
            'SMTP_PASSWORD',
            'SENDER_EMAIL'
        ]
        
        missing = [field for field in required if not getattr(cls, field, None)]
        
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")
        
        if not cls.SUBSCRIBER_EMAILS or cls.SUBSCRIBER_EMAILS == ['']:
            raise ValueError("SUBSCRIBER_EMAILS must contain at least one email")

