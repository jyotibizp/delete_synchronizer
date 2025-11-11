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

**Option A: Using .env file (recommended for local testing)**

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your settings
```

**Option B: Using local.settings.json (Azure Functions format)**

Copy `local.settings.example.json` to `local.settings.json` and configure:

```bash
cp local.settings.example.json local.settings.json
# Edit local.settings.json with your settings
```

**Configuration Example (.env format):**

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.pem
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=IC_CRM_DB
SNOWFLAKE_SCHEMA=IC_CRM

# Email
SMTP_HOST=smtp.yourcompany.com
SMTP_PORT=587
SMTP_USERNAME=your_smtp_username
SMTP_PASSWORD=your_smtp_password
SMTP_USE_TLS=true
SENDER_EMAIL=notifications@yourcompany.com
SUBSCRIBER_EMAILS=email1@example.com,email2@example.com
```

### 3. Authentication Setup

#### For SMTP Email
Contact your IT team for SMTP server details (host, port, credentials).

#### For Snowflake
1. Generate unencrypted key pair:
   ```bash
   openssl genrsa -out rsa_key.pem 2048
   ```
2. Extract public key:
   ```bash
   openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub
   ```
3. Assign public key to Snowflake user:
   ```sql
   ALTER USER your_user SET RSA_PUBLIC_KEY='<public_key_content>';
   ```
4. Set `SNOWFLAKE_PRIVATE_KEY_PATH` to your `rsa_key.pem` file path

**Note:** The private key must be unencrypted (no passphrase).

### 4. Test Locally (Without Azure Functions)

Run the notification engine directly without deploying:

```bash
# Test with yesterday's data
python run_local.py

# Test with specific date
python run_local.py 2024-01-15
```

This will:
- Load settings from `.env` (or `local.settings.json` if `.env` doesn't exist)
- Fetch data from Snowflake for the specified date
- Display a preview of the report
- Send the email via SMTP

### 5. Run as Azure Function Locally

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

## Configuration Details

### Email Settings (SMTP)

| Setting | Required | Notes |
|---------|----------|-------|
| SMTP_HOST | Yes | SMTP server hostname |
| SMTP_PORT | No | Default: 587 |
| SMTP_USERNAME | Yes | SMTP authentication username |
| SMTP_PASSWORD | Yes | SMTP authentication password |
| SMTP_USE_TLS | No | Default: true |
| SENDER_EMAIL | Yes | From email address |
| SUBSCRIBER_EMAILS | Yes | Comma-separated recipient emails |

### Snowflake Settings

| Setting | Required | Notes |
|---------|----------|-------|
| SNOWFLAKE_ACCOUNT | Yes | Your Snowflake account identifier |
| SNOWFLAKE_USER | Yes | Snowflake username |
| SNOWFLAKE_PRIVATE_KEY_PATH | Yes | Path to unencrypted private key PEM file |
| SNOWFLAKE_WAREHOUSE | Yes | Warehouse name |
| SNOWFLAKE_DATABASE | No | Defaults to IC_CRM_DB |
| SNOWFLAKE_SCHEMA | No | Defaults to IC_CRM |

## Troubleshooting

**No emails received:**
- Verify SMTP host and port are correct
- Check username and password
- Ensure TLS/SSL settings match your server
- Check firewall allows outbound connections
- Verify subscriber emails are correct
- Review function logs for errors

**Snowflake connection errors:**
- Verify private key file path is accessible
- Check key format is PEM and unencrypted
- Ensure public key is assigned to user in Snowflake
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
│       ├── email_service.py   # SMTP email service
│       └── email_template.py  # HTML generation
├── scripts/
│   └── setup_venv.sh
├── run_local.py               # Local test runner
├── .env.example               # Environment variables template
├── local.settings.example.json # Azure Functions config template
├── host.json
├── requirements.txt
└── README.md
```

