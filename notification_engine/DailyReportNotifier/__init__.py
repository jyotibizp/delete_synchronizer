import azure.functions as func
import logging
from datetime import datetime, timedelta
from ..src.snowflake.connector import SnowflakeConnector
from ..src.email.email_service import EmailService
from ..src.processors.report_processor import ReportProcessor

def main(mytimer: func.TimerRequest) -> None:
    """
    Azure Function triggered daily to send consolidated execution reports.
    Default schedule: 0 0 8 * * * (8 AM daily)
    """
    logging.info('Daily Report Notifier function started')
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    try:
        # Get yesterday's date for reporting
        yesterday = (datetime.utcnow() - timedelta(days=1)).date()
        logging.info(f'Generating report for date: {yesterday}')
        
        # Initialize services
        snowflake_conn = SnowflakeConnector()
        email_service = EmailService()
        report_processor = ReportProcessor()
        
        # Fetch execution records from Snowflake
        records = snowflake_conn.fetch_daily_executions(yesterday)
        logging.info(f'Retrieved {len(records)} execution records')
        
        if not records:
            logging.info('No execution records found for yesterday. Skipping email.')
            return
        
        # Process records into consolidated report
        report_data = report_processor.consolidate_executions(records)
        
        # Generate and send email
        email_service.send_daily_report(yesterday, report_data)
        
        logging.info('Daily report sent successfully')
        
    except Exception as e:
        logging.error(f'Error in Daily Report Notifier: {str(e)}', exc_info=True)
        raise

