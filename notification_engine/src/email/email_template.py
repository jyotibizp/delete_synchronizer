from datetime import date
from typing import List, Dict, Any

class EmailTemplate:
    """HTML email template generator"""
    
    @staticmethod
    def generate_report_html(report_date: date, report_data: List[Dict[str, Any]]) -> str:
        """
        Generate HTML email content with execution report table.
        
        Args:
            report_date: The date the report covers
            report_data: List of report rows
            
        Returns:
            HTML string
        """
        # Generate table rows
        rows_html = []
        for row in report_data:
            status_class = 'status-success' if row['status'].lower() == 'success' else 'status-error'
            
            rows_html.append(f"""
                <tr>
                    <td>{row['object_name']}</td>
                    <td class="number">{row['inserted']}</td>
                    <td class="number">{row['updated']}</td>
                    <td class="number">{row['deleted']}</td>
                    <td class="{status_class}">{row['status']}</td>
                    <td class="log-message">{row['log_message']}</td>
                </tr>
            """)
        
        table_rows = '\n'.join(rows_html)
        
        # Calculate summary stats
        total_inserted = sum(row['inserted'] for row in report_data)
        total_updated = sum(row['updated'] for row in report_data)
        total_deleted = sum(row['deleted'] for row in report_data)
        failed_count = sum(1 for row in report_data if row['status'].lower() != 'success')
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    border-radius: 8px;
                    padding: 30px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                h1 {{
                    color: #2c3e50;
                    margin-bottom: 10px;
                }}
                .date {{
                    color: #7f8c8d;
                    font-size: 14px;
                    margin-bottom: 20px;
                }}
                .summary {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 6px;
                    margin-bottom: 25px;
                    display: flex;
                    gap: 30px;
                    flex-wrap: wrap;
                }}
                .summary-item {{
                    flex: 1;
                    min-width: 150px;
                }}
                .summary-label {{
                    font-size: 12px;
                    color: #6c757d;
                    text-transform: uppercase;
                    margin-bottom: 5px;
                }}
                .summary-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #2c3e50;
                }}
                .summary-value.error {{
                    color: #dc3545;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 20px;
                }}
                th {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                }}
                td {{
                    padding: 12px;
                    border-bottom: 1px solid #dee2e6;
                }}
                tr:hover {{
                    background-color: #f8f9fa;
                }}
                .number {{
                    text-align: right;
                    font-family: 'Courier New', monospace;
                }}
                .status-success {{
                    color: #28a745;
                    font-weight: 600;
                }}
                .status-error {{
                    color: #dc3545;
                    font-weight: 600;
                }}
                .log-message {{
                    font-size: 13px;
                    color: #6c757d;
                    max-width: 300px;
                    word-wrap: break-word;
                }}
                .footer {{
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #dee2e6;
                    font-size: 12px;
                    color: #6c757d;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Daily Execution Report</h1>
                <div class="date">Report Date: {report_date.strftime('%A, %B %d, %Y')}</div>
                
                <div class="summary">
                    <div class="summary-item">
                        <div class="summary-label">Total Inserted</div>
                        <div class="summary-value">{total_inserted:,}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Total Updated</div>
                        <div class="summary-value">{total_updated:,}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Total Deleted</div>
                        <div class="summary-value">{total_deleted:,}</div>
                    </div>
                    <div class="summary-item">
                        <div class="summary-label">Failed Executions</div>
                        <div class="summary-value{'error' if failed_count > 0 else ''}">{failed_count}</div>
                    </div>
                </div>
                
                <table>
                    <thead>
                        <tr>
                            <th>Object Name</th>
                            <th style="text-align: right;">Inserted</th>
                            <th style="text-align: right;">Updated</th>
                            <th style="text-align: right;">Deleted</th>
                            <th>Status</th>
                            <th>Log Message</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
                
                <div class="footer">
                    This is an automated report generated by the Notification Engine.
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

