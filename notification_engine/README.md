# Notification Engine

Azure Function that sends daily consolidated email reports from the EXECUTION_TRACKER table.

## Overview

This function runs daily at 8 AM UTC, reads execution records from Snowflake, consolidates them by object, and sends a formatted email report to subscribers.

## Features

- **Daily consolidation**: Combines multiple executions per object into a single report
- **Multi-type support**: Handles upsert, delete, and validation execution types
- **HTML email**: Clean, responsive email template with summary statistics
- **Error handling**: Captures and logs failures appropriately

## Report Types

### ADF Upsert (`adf/upsert/Account`)
Aggregates inserted and updated counts from report JSON:
```json
{
  "copied": 100,
  "updated": 20,
  "inserted": 40
}
```

### Azure Function Delete (`azure_func/delete/`)
Aggregates delete counts per object from report JSON:
```json
{
  "Account": 20,
  "Event": 30,
  "Task": 20
}
```

### Azure Function Validation (`azure_func/validation`)
Shows validation status per object:
```json
{
  "Account": "Success",
  "Event": "Success",
  "Task": "Success"
}
```

## Setup

### 1. Install Dependencies

```bash
./scripts/setup_venv.sh
```

Or manually:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Settings

Copy `local.settings.example.json` to `local.settings.json` and fill in:

```json
{
  "Values": {
    "SNOWFLAKE_ACCOUNT": "your_account",
    "SNOWFLAKE_USER": "your_user",
    "SNOWFLAKE_PASSWORD": "your_password",
    "SNOWFLAKE_WAREHOUSE": "your_warehouse",
    "SNOWFLAKE_DATABASE": "IC_CRM_DB",
    "SNOWFLAKE_SCHEMA": "IC_CRM",
    "SENDGRID_API_KEY": "your_sendgrid_api_key",
    "SENDER_EMAIL": "notifications@yourcompany.com",
    "SUBSCRIBER_EMAILS": "email1@example.com,email2@example.com"
  }
}
```

### 3. Get SendGrid API Key

1. Sign up at [SendGrid](https://sendgrid.com/)
2. Create an API key with "Mail Send" permissions
3. Add to `SENDGRID_API_KEY` in settings

### 4. Run Locally

```bash
func start
```

The function runs daily at 8 AM UTC. To test immediately, trigger manually via the Azure Functions portal or adjust the schedule in `DailyReportNotifier/function.json`.

## Schedule Configuration

Default schedule: `0 0 8 * * *` (8 AM UTC daily)

To change, edit `DailyReportNotifier/function.json`:
```json
{
  "schedule": "0 0 8 * * *"
}
```

Format: `{second} {minute} {hour} {day} {month} {day-of-week}`

Examples:
- `0 0 8 * * *` - 8 AM daily
- `0 30 9 * * 1-5` - 9:30 AM weekdays only
- `0 0 */6 * * *` - Every 6 hours

## Deployment

Deploy to Azure Functions (Python 3.9+):

```bash
func azure functionapp publish <function-app-name>
```

Configure application settings in Azure portal with the same values from `local.settings.json`.

## Email Report Format

The email includes:

**Summary Section**
- Total Inserted
- Total Updated  
- Total Deleted
- Failed Executions

**Detail Table**
| Object Name | Inserted | Updated | Deleted | Status | Log Message |
|-------------|----------|---------|---------|--------|-------------|
| Account     | 100      | 20      | 5       | Success|             |
| Event       | 50       | 10      | 2       | Failed | Error details|

## Troubleshooting

**No emails received:**
- Check SendGrid API key is valid
- Verify sender email is authenticated in SendGrid
- Check subscriber emails are correct
- Review function logs for errors

**Connection errors:**
- Verify Snowflake credentials
- Check warehouse is running
- Ensure network access to Snowflake

**No data in report:**
- Confirm executions exist for yesterday's date
- Check INSERTED_DATE column in database
- Review query in `snowflake/connector.py`

## File Structure

```
notification_engine/
├── DailyReportNotifier/       # Timer-triggered function
│   ├── __init__.py
│   └── function.json
├── src/
│   ├── config/
│   │   └── settings.py        # Environment config
│   ├── snowflake/
│   │   └── connector.py       # Database queries
│   ├── processors/
│   │   └── report_processor.py # Data consolidation
│   └── email/
│       ├── email_service.py   # SendGrid integration
│       └── email_template.py  # HTML generation
├── scripts/
│   └── setup_venv.sh
├── host.json
├── requirements.txt
└── README.md
```

