CREATE OR REPLACE PROCEDURE DELETE_account()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
     rows_deleted int;
     return_message string;
BEGIN
  CREATE or replace TEMPORARY TABLE temp_delete_account 
  as
  SELECT ID, OBJECT_NAME, RECORD_ID FROM IC_CRM.DELETE_TRACKER WHERE STATUS = 'open' and OBJECT_NAME = 'Account';

  CREATE or replace TEMPORARY TABLE temp_delete_oldvalue 
  as
  select a.*, 'DELETED' as Status from Account a 
  inner join temp_delete_account b
  on a.id=b.RECORD_ID;

  update DELETE_TRACKER set STATUS = 'applied' where RECORD_ID in (select id from temp_delete_oldvalue) and OBJECT_NAME = 'Account';

  select count(*) into rows_deleted from temp_delete_oldvalue;
  
  merge into Account a
  using temp_delete_account b
  on a.id=b.RECORD_ID
  when matched
      then delete;

  merge into Account_final a
  using temp_delete_account b
  on a.ACCOUNTID=b.RECORD_ID
  when matched
      then delete;
      
  INSERT into HISTORY_ACCOUNT (Id,	Name,	Primary_Contact__c,	Strategic_Account__c, Account_Record_Type_Name__c,	BillingState,	BillingCity,	BillingCountry,	Region__c,	Coverage_Region__c,	Open_Opps__c,	Sales_Person_1__c,	Sales_Person_2__c,	Client_Service_1_Full_Name__c,	Client_Service_2_Full_Name__c,	Exectutive_Account_Manager_Full_Name__c,	Investor_type__c,	Consultant_Investor_Type__c, First_Investment_Date__c,	First_Direct_Lending_Investment_Date__c,	First_RE_Investment_Date__c,	First_Closed_Won_Opp__c,	X18_Digit_ID__c,	RecordTypeId,	activeInvest_direct_lending__c,	activeInvest_GPsolutions__c,	activeInvest_realestate__c,	Blue_Owl_Active_Investments__c,	Strategy_ies_Invested__c,
ACCOUNT_POD__C, ACTIVE_GPSC_INVESTMENT_MM__C, LAST_ACTIVITY_GP_STRATEGIC_CAPITAL__C,
MEETINGS_LAST_6_MONTHS__C, HQ_COUNTRY__C, LastActivityDate,	LastModifiedDate, Effective_from, Effective_to, Status, hash_data
)
select b.Id, b.Name,	b.Primary_Contact__c,	b.Strategic_Account__c,	b.Account_Record_Type_Name__c,	b.BillingState,	b.BillingCity,	b.BillingCountry,	b.Region__c,	b.Coverage_Region__c,	b.Open_Opps__c,	b.Sales_Person_1__c,	b.Sales_Person_2__c,	b.Client_Service_1_Full_Name__c,	b.Client_Service_2_Full_Name__c,	b.Exectutive_Account_Manager_Full_Name__c,	b.Investor_type__c,	b.Consultant_Investor_Type__c, b.First_Investment_Date__c,	b.First_Direct_Lending_Investment_Date__c,	b.First_RE_Investment_Date__c,	b.First_Closed_Won_Opp__c,	b.X18_Digit_ID__c,	b.RecordTypeId,	b.activeInvest_direct_lending__c,	b.activeInvest_GPsolutions__c,	b.activeInvest_realestate__c,	b.Blue_Owl_Active_Investments__c,	b.Strategy_ies_Invested__c,	b.ACCOUNT_POD__C, b.ACTIVE_GPSC_INVESTMENT_MM__C, b.LAST_ACTIVITY_GP_STRATEGIC_CAPITAL__C, b.MEETINGS_LAST_6_MONTHS__C, b.HQ_COUNTRY__C, b.LastActivityDate,	b.LastModifiedDate,b.Effective_from, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LastModifiedDate)), b.Status, b.hash_data from temp_delete_oldvalue b;

INSERT into HISTORY_ACCOUNT_FINAL (ACCOUNTID, ACCOUNTNAME, PRIMARYCONTACT, STRATEGICACCOUNT, ACCOUNTRECORDTYPENAME, HQADDRESS, HQCITY, BILLINGCOUNTRY, GEOGRAPHICREGION, COVERAGEREGION, OPENOPPORTUNITIES, SALESPERSON1, SALESPERSON2, CLIENTSERVICE1, CLIENTSERVICE2, EXECTUTIVEACCOUNTMANAGERFULLNAME, INVESTORTYPE, CONSULTANTINVESTORTYPE, FIRSTINVESTMENTDATE, FIRSTDIRECTLENDINGINVESTMENTDATE, FIRSTREINVESTMENTDATE, FIRSTCLOSEDWONOPP, X18_DIGIT_ID, RecordTypeId, ACTIVEINVESTDIRECTLENDING, ACTIVEINVESTGPSOLUTIONS, ACTIVEINVESTREALESTATE, BLUEOWLACTIVEINVESTMENTS, STRATEGYIESINVESTED, ACCOUNTPOD, "ACTIVEGPSCINVESTMENT(MM)", LASTACTIVITYGPSTRATEGICCAPITAL, MEETINGSLAST6MONTHS, HQCOUNTRY, LastActivityDate, LastModifiedDate, Effective_from, Effective_to, Status, hash_data
)
select b.Id, b.Name,	b.Primary_Contact__c,	b.Strategic_Account__c,	b.Account_Record_Type_Name__c,	b.BillingState,	b.BillingCity,	b.BillingCountry,	b.Region__c,	b.Coverage_Region__c,	b.Open_Opps__c,	b.Sales_Person_1__c,	b.Sales_Person_2__c,	b.Client_Service_1_Full_Name__c,	b.Client_Service_2_Full_Name__c,	b.Exectutive_Account_Manager_Full_Name__c,	b.Investor_type__c,	b.Consultant_Investor_Type__c, b.First_Investment_Date__c,	b.First_Direct_Lending_Investment_Date__c,	b.First_RE_Investment_Date__c,	b.First_Closed_Won_Opp__c,	b.X18_Digit_ID__c,	b.RecordTypeId,	b.activeInvest_direct_lending__c,	b.activeInvest_GPsolutions__c,	b.activeInvest_realestate__c,	b.Blue_Owl_Active_Investments__c,	b.Strategy_ies_Invested__c,	b.ACCOUNT_POD__C, b.ACTIVE_GPSC_INVESTMENT_MM__C, b.LAST_ACTIVITY_GP_STRATEGIC_CAPITAL__C, b.MEETINGS_LAST_6_MONTHS__C, b.HQ_COUNTRY__C, b.LastActivityDate,	b.LastModifiedDate,b.Effective_from, DATEADD(SECOND, -1, TO_TIMESTAMP(b.LastModifiedDate)), b.Status, b.hash_data from temp_delete_oldvalue b;
  
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
