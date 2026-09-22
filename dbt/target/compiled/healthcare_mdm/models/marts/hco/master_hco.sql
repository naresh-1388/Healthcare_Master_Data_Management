-- Egress/Master model for HMDM_DEV.MASTER.HCO
-- Reads the mastered HMDM_DEV.STAGING_mdm.mdm_hco record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "X_hco_address" as "X_hco_address",
    "X_hco_city" as "X_hco_city",
    "X_hco_postal_code" as "X_hco_postal_code",
    "X_hco_country" as "X_hco_country",
    "AlternateName" as "AlternateName",
    "AlternateNameType" as "AlternateNameType",
    "ElectronicAddress" as "ElectronicAddress",
    "X_parent_organization_eid" as "X_parent_organization_eid",
    "X_hierarchy_relationship_type" as "X_hierarchy_relationship_type",
    "X_infac360ls_tax_number" as "X_infac360ls_tax_number"
from HMDM_DEV.STAGING_mdm.mdm_hco