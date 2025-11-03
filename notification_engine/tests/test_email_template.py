"""Unit tests for email template generation"""
import unittest
from datetime import date
from src.email.email_template import EmailTemplate


class TestEmailTemplate(unittest.TestCase):
    """Test HTML email generation"""
    
    def test_generate_report_html(self):
        """Test HTML generation with sample data"""
        report_date = date(2025, 11, 3)
        report_data = [
            {
                'object_name': 'Account',
                'inserted': 100,
                'updated': 20,
                'deleted': 5,
                'status': 'Success',
                'log_message': ''
            },
            {
                'object_name': 'Event',
                'inserted': 50,
                'updated': 10,
                'deleted': 2,
                'status': 'Failed',
                'log_message': 'Connection timeout'
            }
        ]
        
        html = EmailTemplate.generate_report_html(report_date, report_data)
        
        # Check for key elements
        self.assertIn('Daily Execution Report', html)
        self.assertIn('Sunday, November 03, 2025', html)
        self.assertIn('Account', html)
        self.assertIn('Event', html)
        self.assertIn('100', html)
        self.assertIn('Connection timeout', html)
        self.assertIn('status-success', html)
        self.assertIn('status-error', html)
        
        # Check summary stats
        self.assertIn('150', html)  # Total inserted (100 + 50)
        self.assertIn('30', html)   # Total updated (20 + 10)
        self.assertIn('7', html)    # Total deleted (5 + 2)
        self.assertIn('1', html)    # Failed count
    
    def test_empty_report(self):
        """Test HTML generation with empty data"""
        report_date = date(2025, 11, 3)
        report_data = []
        
        html = EmailTemplate.generate_report_html(report_date, report_data)
        
        # Should still contain structure
        self.assertIn('Daily Execution Report', html)
        self.assertIn('0', html)  # All counts should be 0


if __name__ == '__main__':
    unittest.main()

