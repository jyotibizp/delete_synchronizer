#!/usr/bin/env python3
"""
Local test runner for Daily Report Notifier.
Runs the notification engine without deploying to Azure Functions.

Usage:
    python run_local.py                    # Run for yesterday's data
    python run_local.py 2024-01-15         # Run for specific date

Environment Setup:
    Option 1: Create .env file (recommended for local testing)
    Option 2: Create local.settings.json (Azure Functions format)
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_env_file():
    """Load environment variables from .env file"""
    env_file = Path(__file__).parent / '.env'
    
    if not env_file.exists():
        return False
    
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    logging.info('Loaded settings from .env')
    return True

def load_local_settings():
    """Load environment variables from local.settings.json"""
    settings_file = Path(__file__).parent / 'local.settings.json'
    
    if not settings_file.exists():
        return False
    
    with open(settings_file) as f:
        settings = json.load(f)
    
    # Load values into environment
    for key, value in settings.get('Values', {}).items():
        if not key.startswith('_comment'):
            os.environ[key] = str(value)
    
    logging.info('Loaded settings from local.settings.json')
    return True

def load_settings():
    """Load settings from .env or local.settings.json"""
    # Try .env first (more standard for Python)
    if load_env_file():
        return
    
    # Fall back to local.settings.json (Azure Functions format)
    if load_local_settings():
        return
    
    # Neither found
    logging.error('No configuration file found!')
    logging.error('Please create either:')
    logging.error('  - .env (copy from .env.example)')
    logging.error('  - local.settings.json (copy from local.settings.example.json)')
    sys.exit(1)

def run_report(target_date):
    """Run the daily report for a specific date"""
    from src.snowflake.connector import SnowflakeConnector
    from src.email.email_service import EmailService
    from src.processors.report_processor import ReportProcessor
    
    logging.info(f'Generating report for date: {target_date}')
    
    try:
        # Initialize services
        snowflake_conn = SnowflakeConnector()
        email_service = EmailService()
        report_processor = ReportProcessor()
        
        # Fetch execution records from Snowflake
        logging.info('Fetching execution records from Snowflake...')
        records = snowflake_conn.fetch_daily_executions(target_date)
        logging.info(f'Retrieved {len(records)} execution records')
        
        if not records:
            logging.info('No execution records found for this date. No email sent.')
            return
        
        # Process records into consolidated report
        logging.info('Processing and consolidating records...')
        report_data = report_processor.consolidate_executions(records)
        logging.info(f'Consolidated into {len(report_data)} report rows')
        
        # Display report preview
        logging.info('\n--- Report Preview ---')
        for row in report_data:
            logging.info(f"  {row['object_name']}: "
                        f"Inserted={row['inserted']}, "
                        f"Updated={row['updated']}, "
                        f"Deleted={row['deleted']}, "
                        f"Status={row['status']}")
        logging.info('--- End Preview ---\n')
        
        # Generate and send email
        logging.info('Sending email...')
        email_service.send_daily_report(target_date, report_data)
        
        logging.info('✅ Daily report sent successfully!')
        
    except Exception as e:
        logging.error(f'❌ Error running report: {str(e)}', exc_info=True)
        sys.exit(1)
    finally:
        # Close Snowflake connection
        if 'snowflake_conn' in locals():
            snowflake_conn.close()

def main():
    """Main entry point"""
    # Load settings
    load_settings()
    
    # Parse date argument
    if len(sys.argv) > 1:
        try:
            target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        except ValueError:
            logging.error(f'Invalid date format: {sys.argv[1]}. Use YYYY-MM-DD format.')
            sys.exit(1)
    else:
        # Default to yesterday
        target_date = (datetime.now() - timedelta(days=1)).date()
    
    # Run the report
    run_report(target_date)

if __name__ == '__main__':
    main()

