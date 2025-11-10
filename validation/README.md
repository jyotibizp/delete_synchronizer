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

**One script validates everything:**
- ✅ Row count comparison (Salesforce vs Snowflake base/final tables)
- ✅ Delete operation status (pending/applied)
- ✅ History table tracking (deleted records)
- ✅ One consolidated report saved to `EXECUTION_TRACKER`

**Output:**
- Saves to `EXECUTION_TRACKER` table with TYPE='SYNC_VALIDATION'
- Console output with detailed summary
- Status: `SUCCESS`, `WARNING`, or `PENDING`

## Workflow

1. **After Sync:** Run `sync_validator.py` to verify insert/update sync
2. **Execute:** Run delete procedures in Snowflake  
3. **After Delete:** Run `sync_validator.py` again to verify deletes processed

## Object Mappings

| Salesforce | Snowflake Base | Snowflake Final |
|-----------|----------------|-----------------|
| Account | ACCOUNT | ACCOUNT_FINAL |
| Contact | CONTACT | CONTACT_FINAL |
| Opportunity | OPPORTUNITY | OPPORTUNITY_FINAL |
| Legal_Entity__c | LEGALENTITY | LEGALENTITY_FINAL |
| Investment__c | INVESTMENT | INVESTMENT_FINAL |
| LP_Consultant_Relationship__c | LPCONRELATIONSHIP | LPCONRELATIONSHIP_FINAL |
| Event | EVENT | EVENT_FINAL |
| Task | TASK | TASK_FINAL |
| Activity_Content__c | ACTIVITYCONTENT | ACTIVITYCONTENT_FINAL |
| Fund__c | FUND | FUND_FINAL |

## How It Works

All validation results are saved to the `EXECUTION_TRACKER` table:

```sql
-- View recent validations
SELECT TYPE, STATUS, LOG_MESSAGE, INSERTED_DATE 
FROM EXECUTION_TRACKER 
WHERE TYPE = 'SYNC_VALIDATION'
ORDER BY INSERTED_DATE DESC;

-- View detailed report (JSON)
SELECT REPORT 
FROM EXECUTION_TRACKER 
WHERE TYPE = 'SYNC_VALIDATION' 
ORDER BY INSERTED_DATE DESC 
LIMIT 1;
```

**Status Values:**
- `SUCCESS` - All counts match, no pending deletes
- `WARNING` - Count mismatches detected
- `PENDING` - Counts match but deletes are pending

**Report JSON Structure:**
```json
{
  "validation_type": "SYNC_VALIDATION",
  "timestamp": "2025-11-10T...",
  "summary": {
    "total_objects": 10,
    "base_matches": 10,
    "final_matches": 10,
    "all_counts_match": true,
    "total_pending_deletes": 0,
    "total_deleted_in_history": 150
  },
  "delete_tracker_summary": [...],
  "validation_results": [...]
}
```

## Configuration

Update `config.py` with:
- Salesforce private key path
- Salesforce client ID
- Salesforce username

Snowflake connection is pre-configured in the scripts.

