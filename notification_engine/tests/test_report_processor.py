"""Unit tests for report processor"""
import unittest
from datetime import datetime
from src.processors.report_processor import ReportProcessor


class TestReportProcessor(unittest.TestCase):
    """Test report consolidation logic"""
    
    def setUp(self):
        self.processor = ReportProcessor()
    
    def test_upsert_consolidation(self):
        """Test ADF upsert type consolidation"""
        records = [
            {
                'ID': 1,
                'TYPE': 'adf/upsert/Account',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'inserted': 100, 'updated': 20},
                'INSERTED_DATE': datetime.now()
            },
            {
                'ID': 2,
                'TYPE': 'adf/upsert/Account',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'inserted': 50, 'updated': 10},
                'INSERTED_DATE': datetime.now()
            }
        ]
        
        result = self.processor.consolidate_executions(records)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['object_name'], 'Account')
        self.assertEqual(result[0]['inserted'], 150)
        self.assertEqual(result[0]['updated'], 30)
        self.assertEqual(result[0]['deleted'], 0)
    
    def test_delete_consolidation(self):
        """Test delete type consolidation"""
        records = [
            {
                'ID': 1,
                'TYPE': 'azure_func/delete/',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'Account': 20, 'Event': 30, 'Task': 20},
                'INSERTED_DATE': datetime.now()
            }
        ]
        
        result = self.processor.consolidate_executions(records)
        
        self.assertEqual(len(result), 3)
        account_row = next(r for r in result if r['object_name'] == 'Account')
        self.assertEqual(account_row['deleted'], 20)
        self.assertEqual(account_row['inserted'], 0)
    
    def test_validation_consolidation(self):
        """Test validation type consolidation"""
        records = [
            {
                'ID': 1,
                'TYPE': 'azure_func/validation',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'Account': 'Success', 'Event': 'Success', 'Task': 'Failed'},
                'INSERTED_DATE': datetime.now()
            }
        ]
        
        result = self.processor.consolidate_executions(records)
        
        self.assertEqual(len(result), 3)
        task_row = next(r for r in result if r['object_name'] == 'Task')
        self.assertEqual(task_row['status'], 'Failed')
    
    def test_mixed_types(self):
        """Test consolidation of multiple execution types"""
        records = [
            {
                'ID': 1,
                'TYPE': 'adf/upsert/Account',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'inserted': 100, 'updated': 20},
                'INSERTED_DATE': datetime.now()
            },
            {
                'ID': 2,
                'TYPE': 'azure_func/delete/',
                'STATUS': 'Success',
                'LOG_MESSAGE': '',
                'REPORT': {'Account': 5, 'Event': 10},
                'INSERTED_DATE': datetime.now()
            }
        ]
        
        result = self.processor.consolidate_executions(records)
        
        account_row = next(r for r in result if r['object_name'] == 'Account')
        self.assertEqual(account_row['inserted'], 100)
        self.assertEqual(account_row['updated'], 20)
        self.assertEqual(account_row['deleted'], 5)


if __name__ == '__main__':
    unittest.main()

