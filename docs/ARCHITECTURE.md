# Healthcare Master Data Management - Architecture

## 1. Purpose

Master IQVIA-sourced Healthcare Professional (HCP) and Healthcare
Organization (HCO) data through an Informatica MDM hub, so that every
downstream consumer reads one trusted, deduplicated record per HCP/HCO
instead of reconciling raw IQVIA extracts themselves.

This document is the narrative companion to `HMDM_DEV_Cleaned.xlsx`
(the mapping workbook) - that workbook is the source of truth for every
table/column name and transformation rule; this document explains how
the pieces fit together and which system implements which stage.

## 2. Pipeline stages

| # | Stage | Workbook sheet | Implementation |
|---|-------|-----------------|-----------------|
| 1 | Source -> Raw | `Source_Raw` | Databricks: `src/ingestion/src_to_raw_ingestion.py` |
| 2 | Raw -> Landing | `Raw_to_Land` | Databricks: `src/standardization/standardization.py` |
| 2b | Canonical standardization | (cross-cutting) | Databricks: `src/canonical/canonical.py` |
| 3 | Landing -> Staging (DQ) | `Land_to_Stag` | Databricks: `src/dq/data_quality.py` |
| 4 | Staging -> MDM Ingress | `Stg_MDM_Ingress-HCP` / `-HCO` | Databricks: `src/mdm/mdm_ingress.py`, mirrored as dbt models under `dbt/models/marts/{hcp,hco}/mdm_*.sql` |
| 5 | MDM -> Egress/Master | `MDM_HUB_Egress-HCP_Master` / `-HCO_Master` | Databricks: `src/mdm/mdm_egress.py`, mirrored as dbt models `dbt/models/marts/{hcp,hco}/master_*.sql` |
| 6 | Real-time Search-Before-Create | (SBC flow) | `src/api/sbc.py`, `transform_to_mdm_hub.py`, `process_mdm_hub_response.py`, `transform_to_iqvia.py`, `process_iqvia_response.py`, `duplicate_iqvia_records.py` |
| 7 | Real-time IQVIA<->Informatica bridge | (Lambda) | `src/api/download_api.py` |

Stages 1-5 are the scheduled batch pipeline (Databricks notebooks
`03`-`08`, or the Airflow DAG in `airflow/dags/hmdm_pipeline_dag.py`).
Stages 6-7 are the real-time, per-record API flow used when a new HCP/HCO
needs to be checked against the MDM hub before it is created (Search
Before Create), and are exposed via the FastAPI app in `api/`.

### Snowflake bridge stages (after Egress)

| # | Stage | Implementation |
|---|-------|-----------------|
| 8 | Databricks -> Snowflake Sync | `src/snowflake_sync/push_to_snowflake.py` (notebook `10_Snowflake_Sync`) |
| 9 | dbt models (staging views + marts) | `dbt/` project, run via `dbt/run_dbt.sh` |
| 10 | Snowpark master snapshot | `snowflake/snowpark/publish_master_snapshot.py` |

The full pipeline orchestrator (`Databricks/09_Run_Full_Pipeline.py`)
runs all 9 stages end-to-end (stages 1-5 as Databricks notebooks,
then Snowflake Sync, dbt run, and Snowpark snapshot). All paths are
resolved dynamically from the notebook context -- no hard-coded
workspace paths, so it works for any team member.

### External dependencies

1. **API-to-RAW (separate flow):** The AWS Lambda bridge
   (`src/api/download_api.py`) feeds the Informatica MDM Hub, not the
   Databricks pipeline directly. The API-to-RAW caller
   (`src/api/api_to_raw_caller.py`) writes to `raw.hcp_api_data` /
   `raw.hco_api_data`, which are separate from the per-entity raw tables
   (`raw.hcp_name`, `raw.hcp_address`, etc.) consumed by the main
   pipeline. There is no production transformation between them. This
   is by design.

2. **Informatica MDM Hub:** The HCP child master dbt models
   (`master_hcp_specialty`, `master_hcp_alternate_name`,
   `master_hcp_license`, `master_hcp_therapeutic_area`) read from
   `source('mdm_hub', ...)` which are `HMDM_DEV.MDM.*` tables created
   by the Informatica MDM Hub engine, NOT by this repository. These
   tables must exist and be populated before the dbt models that
   depend on them can run successfully.

## 3. Layer naming (Unity Catalog / Snowflake)

```
HMDM_DEV.RAW.*      - one table per Source_Raw target object
HMDM_DEV.LANDING.*  - one table per Raw_to_Land target object
HMDM_DEV.STAGING.*  - one table per Land_to_Stag target object (post-DQ)
HMDM_DEV.MDM.*      - Informatica MDM hub objects (HCP, HCO, + child objects)
HMDM_DEV.MASTER.*   - Egress/downstream-ready mastered data
HMDM_DEV.UTIL.*      - pipeline control/utility tables (see runtime_config.py)
```

See `snowflake/01_create_schemas.sql` through `07_create_control_tables.sql`
for the full DDL, generated directly from the mapping workbook.

## 4. HCP vs HCO consolidation pattern

- **HCP**: 14 STAGING tables (name, address, alternate name, education,
  email, HCO affiliation, identification, language, license, origin
  university, phone, specialty, tax, tendencies) all land as attributes on
  one flat `MDM.HCP` base object. The Informatica MDM hub engine then
  internally derives normalized child/history objects
  (`MDM.HCP_SPECIALTY`, `MDM.HCP_ALTERNATE_NAME`, `MDM.HCP_LICENSE`,
  `MDM.HCP_THERAPEUTIC_AREA`) from those attributes - Egress reads those
  hub-generated objects directly (see the `mdm_hub` dbt source).
- **HCO**: address/alternate-name/email/hierarchy/tax attributes
  consolidate onto core `MDM.HCO` the same way, but name/alternate
  identifier/phone/specialty are written directly into their own
  already-normalized MDM tables by Ingress (`MDM.HCO_NAME`,
  `MDM.HCO_ALTERNATE_IDENTIFIER`, `MDM.HCO_PHONE`, `MDM.HCO_SPECIALTY`),
  so Egress for those four is a pure 1:1 passthrough.

This asymmetry between HCP and HCO is intentional and was confirmed by
cross-referencing every existing Egress block during the mapping audit
(see `Audit_Trail_Log` sheet, rows `#16`/`#17`) - it is not a bug.

## 5. Known open items

See the `Audit_Trail_Log` sheet in `HMDM_DEV_Cleaned.xlsx` for the full,
itemized list. In summary:

- `HCP_ORIGIN_UNIVERSITY`'s RAW-level IQVIA field path and 18 of
  `HCO_NAME`'s attributes (bed count, teaching-hospital flag, Medicare/
  Medicaid acceptance, etc.) are **proposed structures**, not confirmed
  against a real IQVIA API contract - validate before relying on them in
  production.
- `HCP_LICENSE` is sourced via the separate real-time IQVIA<->Informatica
  Lambda integration (`src/api/download_api.py`), not the batch
  Source_Raw pipeline.

## 6. Naming convention (Informatica / IQVIA)

The project uses explicit vendor naming for all MDM attributes:

- **Informatica** -- All Informatica MDM hub attributes use the `X_informatica_*`
  prefix (e.g. `X_informatica_type`, `X_informatica_Specialty`,
  `X_informatica_rank`, `HCO_X_informatica_bedCount`). Earlier versions used
  codenames `infa360` and `infac360ls`; these have been renamed throughout
  the project (Python, SQL, dbt models, Snowflake DDL) to `informatica`.

- **IQVIA** -- All IQVIA-related attributes use the `X_iqvia_*` prefix
  (e.g. `X_iqvia_title`). The JSON path `iqviaTitle` maps to `X_iqvia_title`.
  Earlier versions used the codename `jisb`; this has been renamed throughout
  to `iqvia`.

- **MDM_HUB** -- The Informatica MDM hub search/match API is referenced as
  `MDM_HUB` in `src/api/download_api.py` (previously codenamed `ORIEO`).
  The `IQVIA` API (previously codenamed `JISB`) is the direct IQVIA
  individual-search API.
