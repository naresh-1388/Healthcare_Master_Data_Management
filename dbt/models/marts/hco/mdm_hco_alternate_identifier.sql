-- MDM ingress model for HMDM_DEV.MDM.HCO_ALTERNATE_IDENTIFIER
-- Straight column-level mapping from stg_hco_identification, matching the
-- Stg_MDM_Ingress sheet's src_attribute -> tgt_attribute pairs exactly
-- (one dbt source table -> one MDM target table, no join needed).

select
    "Identifier_Value" as "altValue",
    "Status" as "IdentifierStatus",
    "Identifier_Issuer" as "X_infa360_identifierIssuer",
    "Issuing_Country" as "X_infa360_issuingCountry",
    "Issuing_State" as "X_infa360_issuingState",
    "Activation_Date" as "X_activation_date",
    "Expiration_Date" as "X_expiration_date",
    "Source_Name" as "sourceSystem",
    "Source_FK" as "AlternateIdentifier_parentId"
from {{ ref('stg_HCO_IDENTIFICATION') }}
