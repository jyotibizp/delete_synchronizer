CREATE OR REPLACE PROCEDURE DELETE_task()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
     rows_deleted int;
     return_message string;
BEGIN
  CREATE or replace TEMPORARY TABLE temp_delete_task 
  as
  SELECT ID, OBJECT_NAME, RECORD_ID FROM IC_CRM.DELETE_TRACKER WHERE STATUS = 'open' and OBJECT_NAME = 'Task';

  CREATE or replace TEMPORARY TABLE temp_delete_oldvalue 
  as
  select a.*, 'DELETED' as Status from TASK a 
  inner join temp_delete_task b
  on a.id=b.RECORD_ID;

  update DELETE_TRACKER set STATUS = 'applied' where RECORD_ID in (select id from temp_delete_oldvalue) and OBJECT_NAME = 'Task';

  select count(*) into rows_deleted from temp_delete_oldvalue;
  
  merge into TASK a
  using temp_delete_task b
  on a.id=b.RECORD_ID
  when matched
      then delete;

  merge into TASK_FINAL a
  using temp_delete_task b
  on a.TASKID=b.RECORD_ID
  when matched
      then delete;
      
  INSERT into HISTORY_TASK (ID, "Account.Name", TYPE_OF_ACTIVITY__C, ACTIVITYDATE, "Owner.Name", ASSIGNED_TO_IBD_POD__C, ACCOUNT_POD__C, ASSIGNED_TO_PROFILE__C, CREATED_BY_PROFILE__C, EFFECTIVE_FROM, EFFECTIVE_TO, HASH_DATA, STATUS, LASTMODIFIEDDATE, X18_DIGIT_ACCOUNT_ID__C)
  select b.ID, b."Account.Name", b.TYPE_OF_ACTIVITY__C, b.ACTIVITYDATE, b."Owner.Name", b.ASSIGNED_TO_IBD_POD__C, b.ACCOUNT_POD__C, b.ASSIGNED_TO_PROFILE__C, b.CREATED_BY_PROFILE__C, b.EFFECTIVE_FROM, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LASTMODIFIEDDATE)), b.HASH_DATA, b.Status, b.LASTMODIFIEDDATE, b.X18_DIGIT_ACCOUNT_ID__C from temp_delete_oldvalue b;

  INSERT into HISTORY_TASK_FINAL (TASKID, ACCOUNTNAME, TYPEOFACTIVITY, ACTIVITYDATE, OWNERNAME, ASSIGNEDTOIBDPOD, ACCOUNTPOD, ASSIGNEDTOPROFILE, CREATEDBYPROFILE, EFFECTIVE_FROM, EFFECTIVE_TO, HASH_DATA, STATUS, LASTMODIFIEDDATE, ACCOUNT_ID)
  select b.ID, b."Account.Name", b.TYPE_OF_ACTIVITY__C, b.ACTIVITYDATE, b."Owner.Name", b.ASSIGNED_TO_IBD_POD__C, b.ACCOUNT_POD__C, b.ASSIGNED_TO_PROFILE__C, b.CREATED_BY_PROFILE__C, b.EFFECTIVE_FROM, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LASTMODIFIEDDATE)), b.HASH_DATA, b.Status, b.LASTMODIFIEDDATE, b.X18_DIGIT_ACCOUNT_ID__C from temp_delete_oldvalue b;
  
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

