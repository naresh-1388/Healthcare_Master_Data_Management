
  
    

        create or replace transient table HMDM_DEV.STAGING_mdm.master_hcp_license
         as
        (-- Egress/Master model for HMDM_DEV.MASTER.HCP_LICENSE
-- Reads the Informatica-MDM-hub-exposed child object directly
-- (not a model built by this dbt project - see the 'mdm_hub' source in
-- models/staging/sources.yml) and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "X_infa360_License_parentId" as "Global_HCP_ID",
    "sourcePKey" as "Source_PK",
    "X_infa360_LicenseType" as "License_Type",
    "X_infa360_LicenseNumber" as "License_Number",
    "X_country" as "Country",
    "X_state" as "State",
    "X_sample_elig" as "License_Sample_Eligibility",
    "X_sampleability_overall" as "Sampleability_Overall",
    "X_sampleability_lastreceived_date" as "Sampleability_Last_Received_Date",
    "X_sampleability_fed_sanctions_date" as "Sampleability_Fed_Sanctions_Date",
    "X_sampleability_desigstatus" as "Sampleability_Designation_Status",
    "X_infa360_issueDate" as "Issue_Date",
    "X_infa360_expiryDate" as "Expiry_Date",
    "X_degree" as "Degree",
    "X_adjLic_expdate" as "Adj_License_Exp_Date",
    "X_AdjCode" as "Adj_Code",
    "X_AdjCodesDescriptions" as "Adj_Codes_Descriptions",
    "X_infa360_status" as "License_Status"
from HMDM_DEV.MDM.hcp_license
        );
      
  