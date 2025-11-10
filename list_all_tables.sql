-- Select database and schema
USE DATABASE IC_CRM_DB;
USE SCHEMA IC_CRM;

-- Get all table names in the schema
SELECT 
    TABLE_CATALOG as DATABASE_NAME,
    TABLE_SCHEMA as SCHEMA_NAME,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'IC_CRM'
ORDER BY TABLE_NAME;

-- Alternative: Get tables matching delete patterns only
-- SELECT TABLE_NAME
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'IC_CRM'
--   AND (
--     TABLE_NAME IN ('Account', 'ActivityContent', 'Contact', 'Event', 'Fund', 'Investment', 'LegalEntity', 'LP_Consultant_Relationship', 'Opportunity', 'Task')
--     OR TABLE_NAME LIKE '%_final'
--     OR TABLE_NAME LIKE 'HISTORY_%'
--   )
-- ORDER BY TABLE_NAME;

