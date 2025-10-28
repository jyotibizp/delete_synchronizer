# Delete Sync to Snowflake

Azure Function that syncs Salesforce delete events from SQLite files to Snowflake.

## Overview

Runs every 24 hours to:
1. List all SQLite event files from Azure Blob Storage
2. Download and read events from each file
3. Insert events into Snowflake `delete_tracker` table
4. Skip duplicates automatically

## Prerequisites

- Python 3.9+
- Azure Functions Core Tools v4 (`func`)
- Access to Azure Blob Storage with Salesforce event files
- Snowflake account and database

## Setup

### 1. Create virtual environment
```bash
cd /Users/pradeep/projects/sandbox/sf_delete_tracker/delete_sync
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure settings
```bash
cp local.settings.example.json local.settings.json
# Edit local.settings.json with your credentials
```

### 3. Required environment variables

**Option A: Password Authentication**
```json
{
  "AZURE_STORAGE_CONNECTION_STRING": "connection_string_here",
  "AZURE_BLOB_CONTAINER": "events",
  
  "SNOWFLAKE_ACCOUNT": "your-account.snowflakecomputing.com",
  "SNOWFLAKE_USER": "username",
  "SNOWFLAKE_PASSWORD": "password",
  "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
  "SNOWFLAKE_DATABASE": "YOUR_DB",
  "SNOWFLAKE_SCHEMA": "PUBLIC",
  "SNOWFLAKE_TABLE": "delete_tracker",
  
  "ENVIRONMENT": "local"
}
```

**Option B: RSA Key Authentication (Recommended for Production)**
```json
{
  "AZURE_STORAGE_CONNECTION_STRING": "connection_string_here",
  "AZURE_BLOB_CONTAINER": "events",
  
  "SNOWFLAKE_ACCOUNT": "your-account.snowflakecomputing.com",
  "SNOWFLAKE_USER": "username",
  "SNOWFLAKE_PRIVATE_KEY_PATH": "certs/snowflake_rsa_key.p8",
  "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
  "SNOWFLAKE_DATABASE": "YOUR_DB",
  "SNOWFLAKE_SCHEMA": "PUBLIC",
  "SNOWFLAKE_TABLE": "delete_tracker",
  
  "ENVIRONMENT": "local"
}
```

## RSA Key Authentication Setup (Recommended)

For production environments, use RSA key authentication instead of passwords:

### 1. Generate RSA key pair
```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in snowflake_rsa_key.p8 -pubout -out snowflake_rsa_key.pub
```

### 2. Assign public key to Snowflake user
```sql
-- In Snowflake, run:
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';

-- Verify
DESC USER your_username;
```

### 3. Store private key securely
```bash
mkdir -p certs
mv snowflake_rsa_key.p8 certs/
chmod 600 certs/snowflake_rsa_key.p8
```

### 4. Configure function to use RSA key
In `local.settings.json`:
```json
{
  "SNOWFLAKE_PRIVATE_KEY_PATH": "certs/snowflake_rsa_key.p8"
}
```
(Remove or leave `SNOWFLAKE_PASSWORD` empty)

## Running Locally

### Start the function
```bash
source .venv/bin/activate
func start
```

### Trigger manually
```bash
curl -X POST http://localhost:7071/admin/functions/SnowflakePusher \
  -H 'Content-Type: application/json' -d '{}'
Invoke-RestMethod -Uri "http://localhost:7071/admin/functions/SnowflakePusher" -Method Post -ContentType "application/json" -Body "{}"
```

## Snowflake Table Schema

Auto-created on first run:

```sql
CREATE TABLE delete_tracker (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    replay_id BINARY,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    object_name VARCHAR(255),
    record_id VARCHAR(255),
    deleted_by VARCHAR(255),
    deleted_date TIMESTAMP_NTZ,
    owner_id VARCHAR(255),
    payload VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Deployment

### Deploy to Azure
```bash
az login

# Create resources (one-time)
az group create -n <rg> -l <region>
az storage account create -n <storageName> -g <rg> -l <region> --sku Standard_LRS
az functionapp create -g <rg> -n <appName> -s <storageName> \
  --consumption-plan-location <region> --runtime python --functions-version 4

# Deploy
func azure functionapp publish <appName> --python

# Configure settings
az functionapp config appsettings set -g <rg> -n <appName> --settings \
  AZURE_STORAGE_CONNECTION_STRING='<conn_str>' \
  SNOWFLAKE_ACCOUNT='<account>' \
  SNOWFLAKE_USER='<user>' \
  SNOWFLAKE_PASSWORD='<password>' \
  SNOWFLAKE_WAREHOUSE='<warehouse>' \
  SNOWFLAKE_DATABASE='<database>' \
  SNOWFLAKE_SCHEMA='PUBLIC' \
  SNOWFLAKE_TABLE='delete_tracker'
```

## Schedule

- **Default:** Every 24 hours at midnight UTC (`0 0 0 * * *`)
- Modify `SnowflakePusher/function.json` to change schedule

## Code Structure

```
delete_sync/
├── SnowflakePusher/           # Azure Function
│   ├── __init__.py            # Main orchestration logic
│   └── function.json          # Function configuration
├── src/
│   ├── config/
│   │   └── settings.py        # Environment config
│   ├── storage/
│   │   ├── blob_reader.py     # List & download SQLite files
│   │   └── sqlite_reader.py   # Read events from SQLite
│   └── snowflake/
│       └── connector.py       # Snowflake connection & insert
├── requirements.txt
├── host.json
└── local.settings.json
```

## Features

- **Dual authentication:** Supports password or RSA key authentication
- **Duplicate prevention:** Uses `event_id` uniqueness
- **Error resilience:** Continues processing on file errors
- **Local testing:** Works with local filesystem or Azure Blob
- **Auto-table creation:** Creates Snowflake table if needed
- **Batch processing:** Handles multiple SQLite files
- **Secure:** RSA key auth recommended for production

## Monitoring

Check logs for:
```
"Found N event files to process"
"Read N events from <file>"
"Inserted N events from <file> into Snowflake"
"Completed: processed N files, inserted N total events"
```

## Testing

Query Snowflake to verify:
```sql
SELECT COUNT(*) FROM delete_tracker;
SELECT * FROM delete_tracker ORDER BY ingested_at DESC LIMIT 10;
```
