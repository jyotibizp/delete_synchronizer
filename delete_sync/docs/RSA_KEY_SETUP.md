# Snowflake RSA Key Authentication Setup

Complete guide to setting up RSA key authentication for Snowflake (recommended for production).

## Why RSA Key Authentication?

- **More secure:** No passwords stored in config
- **Service account friendly:** Better for automated processes
- **Key rotation:** Easier to rotate keys without changing code
- **Audit trail:** Better tracking in Snowflake logs

## Step-by-Step Setup

### 1. Generate RSA Key Pair

Generate a 2048-bit RSA key pair in PKCS#8 format (unencrypted):

```bash
# Generate private key in PKCS#8 format
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.p8 -nocrypt

# Generate corresponding public key
openssl rsa -in snowflake_rsa_key.p8 -pubout -out snowflake_rsa_key.pub
```

**Files created:**
- `snowflake_rsa_key.p8` - Private key (keep secure, never commit)
- `snowflake_rsa_key.pub` - Public key (assign to Snowflake user)

### 2. Extract Public Key (for Snowflake)

Remove the header/footer and newlines from the public key:

```bash
# macOS/Linux
grep -v "BEGIN PUBLIC" snowflake_rsa_key.pub | grep -v "END PUBLIC" | tr -d '\n' > snowflake_rsa_key.txt

cat snowflake_rsa_key.txt
```

This gives you a single-line public key like:
```
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...
```

### 3. Assign Public Key to Snowflake User

Connect to Snowflake and run:

```sql
-- Assign public key to service account
ALTER USER svc_delete_tracker 
SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';

-- Verify it was set
DESC USER svc_delete_tracker;

-- Check RSA_PUBLIC_KEY_FP (fingerprint) appears
```

### 4. Grant Snowflake Permissions

Ensure the service account has required permissions:

```sql
-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO USER svc_delete_tracker;

-- Grant database and schema access
GRANT USAGE ON DATABASE YOUR_DB TO USER svc_delete_tracker;
GRANT USAGE ON SCHEMA YOUR_DB.PUBLIC TO USER svc_delete_tracker;

-- Grant table permissions
GRANT CREATE TABLE ON SCHEMA YOUR_DB.PUBLIC TO USER svc_delete_tracker;
GRANT INSERT, SELECT ON TABLE YOUR_DB.PUBLIC.delete_tracker TO USER svc_delete_tracker;

-- Verify grants
SHOW GRANTS TO USER svc_delete_tracker;
```

### 5. Store Private Key Securely

**Local Development:**
```bash
mkdir -p certs
mv snowflake_rsa_key.p8 certs/
chmod 600 certs/snowflake_rsa_key.p8

# Add to .gitignore
echo "certs/snowflake_rsa_key.p8" >> .gitignore
```

**Azure Functions (Production):**
Two options:

**Option A: Azure Key Vault (Recommended)**
```bash
# Upload to Key Vault
az keyvault secret set \
  --vault-name your-keyvault \
  --name snowflake-rsa-key \
  --file certs/snowflake_rsa_key.p8

# Reference in function app
az functionapp config appsettings set \
  -g <rg> -n <app-name> --settings \
  SNOWFLAKE_PRIVATE_KEY_PATH="@Microsoft.KeyVault(SecretUri=https://your-keyvault.vault.azure.net/secrets/snowflake-rsa-key/)"
```

**Option B: Upload with Function**
```bash
# Include in deployment (less secure)
# Add key to certs/ directory and deploy
func azure functionapp publish <appName> --python
```

### 6. Configure Function Settings

**Local (`local.settings.json`):**
```json
{
  "Values": {
    "SNOWFLAKE_ACCOUNT": "abc12345.snowflakecomputing.com",
    "SNOWFLAKE_USER": "svc_delete_tracker",
    "SNOWFLAKE_PRIVATE_KEY_PATH": "certs/snowflake_rsa_key.p8",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
    "SNOWFLAKE_DATABASE": "YOUR_DB",
    "SNOWFLAKE_SCHEMA": "PUBLIC",
    "SNOWFLAKE_TABLE": "delete_tracker"
  }
}
```

**Azure (App Settings):**
```bash
az functionapp config appsettings set -g <rg> -n <app-name> --settings \
  SNOWFLAKE_ACCOUNT="abc12345.snowflakecomputing.com" \
  SNOWFLAKE_USER="svc_delete_tracker" \
  SNOWFLAKE_PRIVATE_KEY_PATH="certs/snowflake_rsa_key.p8" \
  SNOWFLAKE_WAREHOUSE="COMPUTE_WH" \
  SNOWFLAKE_DATABASE="YOUR_DB" \
  SNOWFLAKE_SCHEMA="PUBLIC" \
  SNOWFLAKE_TABLE="delete_tracker"
```

## Testing the Connection

### Test Locally
```bash
# Start function
source .venv/bin/activate
func start

# Trigger manually
curl -X POST http://localhost:7071/admin/functions/SnowflakePusher \
  -H 'Content-Type: application/json' -d '{}'

# Check logs for "Using RSA key authentication for Snowflake"
```

### Test in Snowflake
```sql
-- Check connection history
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE USER_NAME = 'SVC_DELETE_TRACKER'
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 10;

-- Should see AUTHENTICATOR = 'RSA_KEYPAIR'
```

## Troubleshooting

### Error: "Invalid public key"
- Ensure you removed BEGIN/END lines and newlines
- Check key format is correct (single line, base64)

### Error: "JWT token is invalid"
- Private key format may be wrong
- Ensure PKCS#8 format: `openssl pkcs8 -topk8 -inform PEM`
- Check key is unencrypted (`-nocrypt`)

### Error: "File not found"
- Check `SNOWFLAKE_PRIVATE_KEY_PATH` points to correct location
- For Azure, ensure key is uploaded with function or in Key Vault

### Error: "Permission denied"
- Run `chmod 600 certs/snowflake_rsa_key.p8`
- Check file ownership

## Key Rotation

To rotate keys:

1. Generate new key pair
2. Add new public key to user: `ALTER USER ... SET RSA_PUBLIC_KEY='...'`
3. Test with new private key
4. Update function configuration
5. Remove old public key after verification

## Security Best Practices

- ✅ Use RSA key authentication in production
- ✅ Store private keys in Azure Key Vault
- ✅ Use service accounts (not personal users)
- ✅ Restrict grants to minimum required
- ✅ Rotate keys regularly (e.g., every 90 days)
- ✅ Never commit private keys to git
- ✅ Use separate keys per environment (dev/staging/prod)
- ❌ Don't use password authentication in production
- ❌ Don't share keys between services
- ❌ Don't use encrypted keys without password management

