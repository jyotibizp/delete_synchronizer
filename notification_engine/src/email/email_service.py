import logging
from datetime import date
from typing import List, Dict, Any
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
from ..config.settings import Settings
from .email_template import EmailTemplate

class EmailService:
    """Handles email notifications via SendGrid"""
    
    def __init__(self):
        Settings.validate()
        self.client = SendGridAPIClient(Settings.SENDGRID_API_KEY)
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
        
        # Create email
        message = Mail(
            from_email=Email(self.sender),
            to_emails=[To(email) for email in self.subscribers],
            subject=subject,
            html_content=Content("text/html", html_content)
        )
        
        try:
            response = self.client.send(message)
            logging.info(f'Email sent successfully. Status code: {response.status_code}')
            logging.info(f'Sent to: {", ".join(self.subscribers)}')
            
        except Exception as e:
            logging.error(f'Failed to send email: {str(e)}')
            raise

