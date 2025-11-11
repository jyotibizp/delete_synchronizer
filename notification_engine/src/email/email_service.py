import logging
import smtplib
from datetime import date
from typing import List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ..config.settings import Settings
from .email_template import EmailTemplate

class EmailService:
    """Handles email notifications via SMTP"""
    
    def __init__(self):
        Settings.validate()
        self.sender = Settings.SENDER_EMAIL
        self.subscribers = [email.strip() for email in Settings.SUBSCRIBER_EMAILS if email.strip()]
    
    def send_daily_report(self, report_date: date, report_data: List[Dict[str, Any]]):
        """
        Send daily consolidated execution report to subscribers.
        
        Args:
            report_date: The date the report covers
            report_data: List of report rows with keys: object_name, inserted, updated, deleted, status, log_message
        """
        if not self.subscribers:
            logging.warning('No subscribers configured. Skipping email.')
            return
        
        # Generate email content
        subject = f"Daily Execution Report - {report_date.strftime('%Y-%m-%d')}"
        html_content = EmailTemplate.generate_report_html(report_date, report_data)
        
        # Create message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = self.sender
        message['To'] = ', '.join(self.subscribers)
        
        # Attach HTML content
        html_part = MIMEText(html_content, 'html')
        message.attach(html_part)
        
        try:
            # Connect to SMTP server and send
            with smtplib.SMTP(Settings.SMTP_HOST, Settings.SMTP_PORT) as server:
                if Settings.SMTP_USE_TLS:
                    server.starttls()
                
                server.login(Settings.SMTP_USERNAME, Settings.SMTP_PASSWORD)
                server.sendmail(self.sender, self.subscribers, message.as_string())
            
            logging.info(f'Email sent successfully via SMTP ({Settings.SMTP_HOST}:{Settings.SMTP_PORT})')
            logging.info(f'Sent to: {", ".join(self.subscribers)}')
            
        except Exception as e:
            logging.error(f'Failed to send email via SMTP: {str(e)}')
            raise

