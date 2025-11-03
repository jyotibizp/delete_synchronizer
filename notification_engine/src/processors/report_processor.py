import json
import logging
from typing import List, Dict, Any

class ReportProcessor:
    """Process execution records into consolidated report format"""
    
    def consolidate_executions(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Consolidate multiple execution records into report rows.
        Each row contains: Object Name, Inserted, Updated, Deleted, Status, Log Message
        
        Args:
            records: List of execution records from database
            
        Returns:
            List of consolidated report rows
        """
        consolidated = {}
        
        for record in records:
            record_type = record.get('TYPE', '')
            status = record.get('STATUS', '')
            log_message = record.get('LOG_MESSAGE', '')
            report_data = record.get('REPORT')
            
            # Parse the report JSON if it exists
            if report_data:
                if isinstance(report_data, str):
                    try:
                        report_data = json.loads(report_data)
                    except json.JSONDecodeError:
                        logging.warning(f'Failed to parse REPORT for record {record.get("ID")}')
                        report_data = {}
            else:
                report_data = {}
            
            # Process based on type
            if 'adf/upsert/' in record_type:
                self._process_upsert(record_type, status, log_message, report_data, consolidated)
            
            elif 'azure_func/delete/' in record_type:
                self._process_delete(record_type, status, log_message, report_data, consolidated)
            
            elif 'azure_func/validation' in record_type:
                self._process_validation(record_type, status, log_message, report_data, consolidated)
            
            else:
                # Generic fallback
                object_name = record_type.split('/')[-1] if '/' in record_type else record_type
                key = object_name
                
                if key not in consolidated:
                    consolidated[key] = {
                        'object_name': object_name,
                        'inserted': 0,
                        'updated': 0,
                        'deleted': 0,
                        'status': status,
                        'log_message': log_message
                    }
        
        # Convert to list and sort by object name
        report_list = list(consolidated.values())
        report_list.sort(key=lambda x: x['object_name'])
        
        return report_list
    
    def _process_upsert(self, record_type: str, status: str, log_message: str, 
                        report_data: dict, consolidated: dict):
        """Process ADF upsert type records"""
        # Extract object name from type like "adf/upsert/Account"
        object_name = record_type.split('/')[-1] if '/' in record_type else 'Unknown'
        
        key = object_name
        
        if key not in consolidated:
            consolidated[key] = {
                'object_name': object_name,
                'inserted': 0,
                'updated': 0,
                'deleted': 0,
                'status': status,
                'log_message': log_message
            }
        
        # Accumulate counts
        consolidated[key]['inserted'] += report_data.get('inserted', 0)
        consolidated[key]['updated'] += report_data.get('updated', 0)
        
        # Update status if current execution failed
        if status.lower() != 'success':
            consolidated[key]['status'] = status
            if log_message:
                consolidated[key]['log_message'] = log_message
    
    def _process_delete(self, record_type: str, status: str, log_message: str,
                        report_data: dict, consolidated: dict):
        """Process delete type records - report contains object names as keys with counts"""
        for object_name, count in report_data.items():
            key = object_name
            
            if key not in consolidated:
                consolidated[key] = {
                    'object_name': object_name,
                    'inserted': 0,
                    'updated': 0,
                    'deleted': 0,
                    'status': status,
                    'log_message': log_message
                }
            
            # Accumulate delete counts
            consolidated[key]['deleted'] += int(count) if isinstance(count, (int, float, str)) else 0
            
            # Update status
            if status.lower() != 'success':
                consolidated[key]['status'] = status
                if log_message:
                    consolidated[key]['log_message'] = log_message
    
    def _process_validation(self, record_type: str, status: str, log_message: str,
                           report_data: dict, consolidated: dict):
        """Process validation type records - report contains object names with Success/Failure"""
        for object_name, validation_status in report_data.items():
            key = object_name
            
            if key not in consolidated:
                consolidated[key] = {
                    'object_name': object_name,
                    'inserted': 0,
                    'updated': 0,
                    'deleted': 0,
                    'status': validation_status,
                    'log_message': log_message if validation_status.lower() != 'success' else ''
                }
            else:
                # Update status if validation failed
                if validation_status.lower() != 'success':
                    consolidated[key]['status'] = validation_status
                    if log_message:
                        consolidated[key]['log_message'] = log_message

