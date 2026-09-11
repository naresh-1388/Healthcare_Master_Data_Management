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
Before Create), and are exposed via the Flask app in `api/`.

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

## 6. Naming note (ORIEO / JISB)

Earlier versions of `src/api/*.py` used the names `ORIEO` and `JISB` for
the two search/match interfaces used by the Search-Before-Create flow.
These were confirmed to be leftover names from an unrelated
client/template and have been renamed throughout to `MDM_HUB` (the
Informatica MDM hub's own search/match API) and `IQVIA` (the direct
IQVIA individual-search API) respectively, to stay consistent with the
rest of this project's naming. The one exception is the `X_jisb_title`
attribute on `MDM.HCP`, which was already an established, audited
attribute name in the mapping workbook before this rename and was left
unchanged - flag this to your Informatica admin if you'd like it
renamed too, since it is a real attribute name in the MDM base object
model, not just a code-level identifier.
