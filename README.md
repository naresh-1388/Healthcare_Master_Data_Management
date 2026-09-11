# Healthcare Master Data Management (HMDM)

Master IQVIA-sourced Healthcare Professional (HCP) and Healthcare
Organization (HCO) data through an Informatica MDM hub on
Databricks + Snowflake.

**Confidential - internal project. Do not publish or share outside your
organization.**

## What this is

A medallion-architecture pipeline (Source -> Raw -> Landing -> Staging ->
MDM -> Master) plus a real-time Search-Before-Create API integration,
built around the mapping specification in `HMDM_DEV_Cleaned.xlsx`. See
`docs/ARCHITECTURE.md` for the full narrative and
`Audit_Trail_Log` (a sheet inside the workbook) for every design decision
and open item.

## Repository layout

```
src/                  Core PySpark/Databricks pipeline logic (importable, unit-testable)
  core/               Shared runtime config, Delta/DBFS I/O, logging
  ingestion/          Stage 1: Source -> Raw
  standardization/    Stage 2: Raw -> Landing
  canonical/          Stage 2b: cross-source canonical standardization
  dq/                 Stage 3: Landing -> Staging (Data Quality rules)
  staging/            Staging-layer helpers
  mdm/                Stage 4/5: MDM Ingress + Egress
  api/                Real-time Search-Before-Create + IQVIA<->Informatica bridge
Databricks/           Notebook wrappers around src/ (run these interactively or as Jobs)
api/                  Mock IQVIA-shaped REST API (FastAPI) for local dev/testing
snowflake/            DDL for every layer (RAW/LANDING/STAGING/MDM/MASTER/UTIL)
dbt/                  dbt project modelling Staging -> MDM -> Master on Snowflake
airflow/dags/         Optional Airflow DAG (alternative to native Databricks Workflows)
informatica/          CSV exports of the Ingress/Egress mapping sheets
docs/                 Architecture documentation
HMDM_DEV_Cleaned.xlsx The mapping workbook - source of truth for every table/column/rule
```

## Running the pipeline

### Option A: Databricks Workflows (recommended)
1. Import this repo as a Databricks Repo.
2. Create one Databricks Job per notebook in `Databricks/03_*` through
   `08_*`, or a single Job that runs `Databricks/09_Run_Full_Pipeline.py`.
3. Attach a cluster with the Databricks Runtime (Delta Lake is built in -
   do not add `delta-spark` from requirements.txt to the cluster).
4. Run interactively cell-by-cell first to validate against your
   environment, then schedule.

### Option B: Airflow
1. Copy `airflow/dags/hmdm_pipeline_dag.py` into your Airflow DAGs folder.
2. Create the corresponding Databricks Jobs (one per stage) and fill in
   the real `JOB_IDS` in the DAG file.
3. Configure an Airflow connection named `databricks_default`.

### Option C: dbt (Staging -> MDM -> Master only)
```
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in real Snowflake credentials
dbt deps
dbt run
dbt test
```

### Mock API (local development without live IQVIA credentials)
```
pip install -r requirements.txt
cd api
uvicorn app:app --reload
```

## Snowflake setup
Run the scripts in `snowflake/` in order (`01_create_schemas.sql` through
`07_create_control_tables.sql`) against your `HMDM_DEV` database before
the first pipeline run.

## Known open items
See the `Audit_Trail_Log` sheet in `HMDM_DEV_Cleaned.xlsx` for the full
list. The two most important:
- `HCP_ORIGIN_UNIVERSITY`'s source field path and 18 of `HCO_NAME`'s
  facility-profile attributes are **proposed, not yet confirmed** against
  the real IQVIA API contract.
- `HCP_LICENSE` is sourced via the separate real-time IQVIA<->Informatica
  Lambda bridge (`src/api/download_api.py`), not the batch pipeline.
