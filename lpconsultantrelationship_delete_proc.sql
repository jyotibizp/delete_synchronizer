CREATE OR REPLACE PROCEDURE DELETE_lpconsultantrelationship()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
     rows_deleted int;
     return_message string;
BEGIN
  CREATE or replace TEMPORARY TABLE temp_delete_lpconsultantrelationship 
  as
  SELECT ID, OBJECT_NAME, RECORD_ID FROM IC_CRM.DELETE_TRACKER WHERE STATUS = 'open' and OBJECT_NAME = 'LP_Consultant_Relationship';

  CREATE or replace TEMPORARY TABLE temp_delete_oldvalue 
  as
  select a.*, 'DELETED' as Status from LPCONRELATIONSHIP a 
  inner join temp_delete_lpconsultantrelationship b
  on a.X18_DIGIT_ID__C=b.RECORD_ID;

  update DELETE_TRACKER set STATUS = 'applied' where RECORD_ID in (select X18_DIGIT_ID__C from temp_delete_oldvalue) and OBJECT_NAME = 'LP_Consultant_Relationship';

  select count(*) into rows_deleted from temp_delete_oldvalue;
  
  merge into LPCONRELATIONSHIP a
  using temp_delete_lpconsultantrelationship b
  on a.X18_DIGIT_ID__C=b.RECORD_ID
  when matched
      then delete;

  merge into LPCONRELATIONSHIP_FINAL a
  using temp_delete_lpconsultantrelationship b
  on a.ID=b.RECORD_ID
  when matched
      then delete;
      
  INSERT into HISTORY_LPCONRELATIONSHIP (X18_DIGIT_ID__C, NAME, CONSULTANT_NAME__C, CONSULTANT_ROLE__C, CONSULTANT_DRIVEN_OR__C, CREATEDDATE, CONSULTANT_RANKING__C, LASTMODIFIEDDATE, EFFECTIVE_FROM, EFFECTIVE_TO, HASH_DATA, STATUS)
  select b.X18_DIGIT_ID__C, b.NAME, b.CONSULTANT_NAME__C, b.CONSULTANT_ROLE__C, b.CONSULTANT_DRIVEN_OR__C, b.CREATEDDATE, b.CONSULTANT_RANKING__C, b.LASTMODIFIEDDATE, b.EFFECTIVE_FROM, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LASTMODIFIEDDATE)), b.HASH_DATA, b.Status from temp_delete_oldvalue b;

  INSERT into HISTORY_LPCONRELATIONSHIP_FINAL (ID, LPCONSULTANTRELATIONSHIPNUMBER, CONSULTANTNAME, CONSULTANTROLE, DRIVENORADVISING, LPCONSULTANTRELATIONSHIPCREATEDDATE, CONSULTANTRANKING, LPCONSULTANTRELATIONSHIPLASTMODIFIEDDATE, EFFECTIVE_FROM, EFFECTIVE_TO, HASH_DATA, STATUS)
  select b.X18_DIGIT_ID__C, b.NAME, b.CONSULTANT_NAME__C, b.CONSULTANT_ROLE__C, b.CONSULTANT_DRIVEN_OR__C, b.CREATEDDATE, b.CONSULTANT_RANKING__C, b.LASTMODIFIEDDATE, b.EFFECTIVE_FROM, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LASTMODIFIEDDATE)), b.HASH_DATA, b.Status from temp_delete_oldvalue b;
  
   return_message := 'SUCCESS' || ',' || to_varchar(rows_deleted);
  
  RETURN :return_message;
  EXCEPTION
          WHEN STATEMENT_ERROR THEN
            return_message := OBJECT_CONSTRUCT(
              'Error Type', 'STATEMENT_ERROR',
              'SQLCODE', SQLCODE,
              'SQLERRM', SQLERRM,
              'SQLSTATE', SQLSTATE
            );
            ROLLBACK;
            RETURN :return_message;
          WHEN OTHER THEN
            return_message := OBJECT_CONSTRUCT(
              'Error Type', 'STATEMENT_ERROR',
              'SQLCODE', SQLCODE,
              'SQLERRM', SQLERRM,
              'SQLSTATE', SQLSTATE
            );
            ROLLBACK;
            RETURN :return_message;
END;
$$;

