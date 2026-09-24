-- Egress/Master model for HMDM_DEV.MASTER.HCO_ALTERNATE_IDENTIFIER
-- Reads the mastered {{ ref('mdm_hco_alternate_identifier') }} record and applies the
-- exact src_attribute -> tgt_attribute renaming documented in the
-- MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheet for this
-- table (business-friendly names on the HCP side, e.g. firstName ->
-- First_Name; pure passthrough on the HCO side - both preserved exactly
-- as audited in the workbook).

select
    "altValue" as "altValue",
    "IdentifierStatus" as "IdentifierStatus",
    "X_informatica_identifierIssuer" as "X_informatica_identifierIssuer",
    "X_informatica_issuingCountry" as "X_informatica_issuingCountry",
    "X_informatica_issuingState" as "X_informatica_issuingState",
    "X_activation_date" as "X_activation_date",
    "X_expiration_date" as "X_expiration_date",
    "AlternateIdentifier_parentId" as "AlternateIdentifier_parentId"
from {{ ref('mdm_hco_alternate_identifier') }}
