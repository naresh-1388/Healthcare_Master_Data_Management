
  
    

        create or replace transient table HMDM_DEV.STAGING_mdm.master_hcp_specialty
         as
        (-- Egress/Master model for HMDM_DEV.MASTER.HCP_SPECIALTY
-- Reads the Informatica-MDM-hub-exposed child object directly
-- (not a model built by this dbt project - see the 'mdm_hub' source in
-- models/staging/sources.yml) and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "X_infa360_SpecialtyType" as "Specialty",
    "X_infa360_SpecialtyClass" as "Specialty_Type",
    "X_infa360_SpecialtyRank" as "Specialty_Rank",
    "X_infa360_taxonomyName" as "Taxonomy_Name",
    "X_infa360_group" as "Group",
    "X_infa360_taxonomy_code" as "Taxonomy_Code",
    "X_infa360_subClassification" as "Sub_Classification",
    "X_specialty_status" as "Specialty_Status",
    "O_Load_Date" as "Load_Date"
from HMDM_DEV.MDM.hcp_specialty
        );
      
  