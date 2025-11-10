# Azure Function Deployment Guide

## Local Development

1. **Install Azure Functions Core Tools:**
```bash
brew tap azure/functions
brew install azure-functions-core-tools@4
```

2. **Setup environment:**
```bash
cd validation
cp local.settings.example.json local.settings.json
# Edit local.settings.json with your credentials
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Run locally:**
```bash
func start
```

5. **Test locally:**
```bash
curl http://localhost:7071/api/SyncValidator
```

## Azure Deployment

### 1. Create Azure Function App

```bash
# Login to Azure
az login

# Create resource group (if needed)
az group create --name rg-crm-sync --location eastus

# Create storage account
az storage account create \
  --name stcrmsyncvalidator \
  --resource-group rg-crm-sync \
  --location eastus \
  --sku Standard_LRS

# Create Function App (Python 3.11)
az functionapp create \
  --resource-group rg-crm-sync \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name func-crm-sync-validator \
  --storage-account stcrmsyncvalidator \
  --os-type Linux
```

### 2. Configure Application Settings

Add environment variables to Function App:

```bash
az functionapp config appsettings set \
  --name func-crm-sync-validator \
  --resource-group rg-crm-sync \
  --settings \
    SF_CLIENT_ID="your-client-id" \
    SF_USERNAME="your-username" \
    SF_LOGIN_URL="https://login.salesforce.com" \
    SF_PRIVATE_KEY="base64-encoded-key" \
    SNOWFLAKE_ACCOUNT="NP91221-IC_IBD_CRM" \
    SNOWFLAKE_USER="your-username" \
    SNOWFLAKE_PASSWORD="your-password" \
    SNOWFLAKE_ROLE="IC_CRM_DEVELOPER" \
    SNOWFLAKE_WAREHOUSE="IC_CRM_WH_XS" \
    SNOWFLAKE_DATABASE="IC_CRM_DB" \
    SNOWFLAKE_SCHEMA="IC_CRM"
```

### 3. Deploy Function

```bash
cd validation
func azure functionapp publish func-crm-sync-validator
```

### 4. Get Function URL

```bash
func azure functionapp list-functions func-crm-sync-validator --show-keys
```

## Trigger Function

### Manual Trigger (HTTP)

```bash
curl -X POST https://func-crm-sync-validator.azurewebsites.net/api/SyncValidator?code=<function-key>
```

### Scheduled Trigger (Optional)

To run validation automatically, add Timer trigger to `function.json`:

```json
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "timer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "0 0 */6 * * *"
    }
  ]
}
```

Schedule format (CRON):
- `0 0 */6 * * *` - Every 6 hours
- `0 0 0 * * *` - Daily at midnight
- `0 0 8 * * 1-5` - Weekdays at 8 AM

## View Results

### Check Logs

```bash
az functionapp log tail --name func-crm-sync-validator --resource-group rg-crm-sync
```

### Query Snowflake

```sql
SELECT TYPE, STATUS, LOG_MESSAGE, INSERTED_DATE 
FROM EXECUTION_TRACKER 
WHERE TYPE = 'SYNC_VALIDATION'
ORDER BY INSERTED_DATE DESC 
LIMIT 10;
```

## Monitoring

- View Application Insights in Azure Portal
- Check Function execution history
- Monitor Snowflake `EXECUTION_TRACKER` table

## Troubleshooting

**Connection Issues:**
- Verify environment variables are set correctly
- Check Snowflake firewall allows Azure IPs
- Verify Salesforce Connected App has correct permissions

**Timeout Issues:**
- Increase function timeout in `host.json` (max 10 minutes for Consumption plan)
- Consider Premium plan for longer execution times

**Memory Issues:**
- Monitor memory usage in Application Insights
- Consider scaling to Premium plan if needed

