-- ============================================================
-- Healthcare_Master_Data_Management - Schema (Unity Catalog / Snowflake Database) setup
-- One schema per medallion layer, matching the HMDM_DEV workbook's
-- fully-qualified table naming (HMDM_DEV.<LAYER>.<TABLE>).
-- ============================================================
CREATE DATABASE IF NOT EXISTS HMDM_DEV;

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.RAW
    COMMENT = 'Raw layer - one table per Source_Raw sheet target object, minimally transformed from the IQVIA API/file payload.';

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.LANDING
    COMMENT = 'Landing layer - Raw_to_Land sheet: Source_FK + surrogate PK + standardized column names + audit columns.';

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.STAGING
    COMMENT = 'Staging layer - Land_to_Stag sheet: Landing data that has passed the configured DQ rules.';

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.MDM
    COMMENT = 'Informatica MDM hub layer - Stg_MDM_Ingress-HCP / Stg_MDM_Ingress-HCO sheets: mastered HCP/HCO base objects and child objects.';

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.MASTER
    COMMENT = 'Egress/downstream layer - MDM_HUB_Egress-HCP_Master / MDM_HUB_Egress-HCO_Master sheets: mastered data exposed to downstream consumers.';

CREATE SCHEMA IF NOT EXISTS HMDM_DEV.UTIL
    COMMENT = 'Pipeline control/utility tables: batch control, rule master tables, execution logs, mailing lists, DQ rejects.';
