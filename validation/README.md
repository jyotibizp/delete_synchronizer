# Sync Validator - Azure Function

Validates Salesforce-Snowflake data synchronization after insert, update, and delete operations.

## Deployment Options

### Option 1: Azure Function (Recommended for Production)

Deploy as HTTP-triggered Azure Function. See [DEPLOYMENT.md](DEPLOYMENT.md) for complete guide.

**Quick Deploy:**
```bash
cd validation
func azure functionapp publish func-crm-sync-validator
```

**Trigger:**
```bash
curl https://func-crm-sync-validator.azurewebsites.net/api/SyncValidator?code=<key>
```

### Option 2: Local Execution

**Setup:**
```bash
cd validation
pip install -r requirements.txt
python sync_validator.py
```

## What It Validates

**Smart, data-driven validation:**
- ✅ Only validates objects that synced successfully (checks EXECUTION_TRACKER)
- ✅ Row count comparison (Salesforce vs Snowflake staging/final tables)
- ✅ Delete operation status (pending/applied)
- ✅ History table tracking (deleted records)
- ✅ Uses custom queries from ENTITYMAPPING table
- ✅ Per-object reports saved to `EXECUTION_TRACKER`

**Output:**
- One report per object in `EXECUTION_TRACKER` with TYPE='SYNC_VALIDATION'
- Each object gets its own STATUS: `SUCCESS`, `WARNING`, or `PENDING`
- Console output with per-object status

## Workflow

1. **After Sync:** Run `sync_validator.py` to verify insert/update sync
2. **Execute:** Run delete procedures in Snowflake  
3. **After Delete:** Run `sync_validator.py` again to verify deletes processed

## Configuration

Entity mappings are stored in the `ENTITYMAPPING` table with these columns:
- `ENTITYNAME` - Entity identifier
- `MAPPINGTO_SALESFORCE` - Salesforce object name
- `SNOWFLAKE_TABLENAME` - Base table in Snowflake
- `COUNT_SOQL` - Query to count rows in Salesforce
- `COUNT_STAGING` - Query to count rows in Snowflake base table
- `COUNT_FINAL` - Query to count rows in Snowflake final table

The validator dynamically loads all entities from this table and executes the stored queries.

## How It Works

Each object gets its own validation report saved to `EXECUTION_TRACKER`:

```sql
-- View recent validations (all objects)
SELECT OBJECT_NAME, STATUS, LOG_MESSAGE, INSERTED_DATE 
FROM EXECUTION_TRACKER 
WHERE TYPE = 'SYNC_VALIDATION'
ORDER BY INSERTED_DATE DESC;

-- View validation for specific object
SELECT OBJECT_NAME, STATUS, LOG_MESSAGE, REPORT, INSERTED_DATE
FROM EXECUTION_TRACKER 
WHERE TYPE = 'SYNC_VALIDATION' 
  AND OBJECT_NAME = 'Account'
ORDER BY INSERTED_DATE DESC 
LIMIT 1;

-- Summary of latest validation run
SELECT 
    OBJECT_NAME,
    STATUS,
    LOG_MESSAGE,
    INSERTED_DATE
FROM EXECUTION_TRACKER
WHERE TYPE = 'SYNC_VALIDATION'
  AND INSERTED_DATE >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
ORDER BY OBJECT_NAME;
```

**Status Values (per object):**
- `SUCCESS` - All counts match, no pending deletes
- `WARNING` - Count mismatches detected
- `PENDING` - Counts match but deletes are pending

**Per-Object Report Structure:**
```json
{
  "validation_type": "SYNC_VALIDATION",
  "timestamp": "2025-11-13T14:30:45.123456",
  "entity_name": "Account",
  "object": "Account",
  "counts": {
    "salesforce": 1250,
    "snowflake_staging": 1250,
    "snowflake_final": 1245
  },
  "matches": {
    "staging_match": true,
    "final_match": false
  },
  "differences": {
    "staging_diff": 0,
    "final_diff": -5
  },
  "deletes": {
    "pending_deletes": 5,
    "history_total": 50,
    "history_deleted": 20
  },
  "status": "PENDING",
  "log_message": "Account: Counts match but 5 deletes pending."
}
```

## Environment Setup

Set these environment variables or update `.env` file:

**Salesforce:**
- `SF_CLIENT_ID` - Connected app client ID
- `SF_USERNAME` - Salesforce username
- `SF_LOGIN_URL` - Login URL (default: https://login.salesforce.com)
- `SF_PRIVATE_KEY` - Private key content (or place in `certs/private.key`)

**Snowflake:**
- `SNOWFLAKE_ACCOUNT` - Account identifier
- `SNOWFLAKE_USER` - Username
- `SNOWFLAKE_ROLE` - Role to use
- `SNOWFLAKE_WAREHOUSE` - Warehouse name
- `SNOWFLAKE_DATABASE` - Database name
- `SNOWFLAKE_SCHEMA` - Schema name
- `SNOWFLAKE_PRIVATE_KEY` - Private key content (or place in `certs/rsa_key.p8`)

