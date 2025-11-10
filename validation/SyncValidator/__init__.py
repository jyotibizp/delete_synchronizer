"""
Azure Function: Sync Validator
Validates Salesforce-Snowflake data synchronization
"""

import logging
import json
import os
import sys
import azure.functions as func

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.sync_validation_core import run_sync_validation


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function HTTP trigger for sync validation
    
    Usage:
        POST/GET https://<function-app>.azurewebsites.net/api/SyncValidator
    """
    logging.info('Sync Validator function triggered')
    
    try:
        # Run validation
        report_data = run_sync_validation()
        
        # Return response
        response_body = {
            'success': True,
            'status': report_data['summary'].get('status', 'COMPLETED'),
            'message': 'Validation completed successfully',
            'summary': report_data['summary'],
            'timestamp': report_data['timestamp']
        }
        
        logging.info(f"Validation completed: {report_data['summary']}")
        
        return func.HttpResponse(
            body=json.dumps(response_body, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        error_msg = f"Validation failed: {str(e)}"
        logging.error(error_msg, exc_info=True)
        
        return func.HttpResponse(
            body=json.dumps({
                'success': False,
                'error': error_msg
            }),
            status_code=500,
            mimetype="application/json"
        )

