"""
Healthcare MDM - Airflow DAG.

Orchestrates the same 6-stage pipeline as
Databricks/09_Run_Full_Pipeline.py, but as an Airflow DAG that triggers
each Databricks notebook as a job run via the Databricks Airflow provider.
Use this DAG instead of the Databricks Workflow scheduler when the
pipeline needs to be coordinated alongside other (non-Databricks) systems,
or when your team standardizes orchestration on Airflow.

Stage order mirrors the medallion architecture in the HMDM_DEV mapping
workbook: Source -> Raw -> Landing -> (canonical) -> Staging -> MDM Ingress
(HCP, HCO) -> MDM Egress (HCP Master, HCO Master).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = "databricks_default"
JOB_IDS = {
    # Replace these with the real Databricks Job IDs created for each
    # notebook (Workflows -> Jobs -> <job> -> copy the numeric Job ID).
    # Keeping them as separate jobs (rather than one big job) lets each
    # stage be retried/backfilled independently from the Airflow UI.
    "ingestion": "<DATABRICKS_JOB_ID_INGESTION>",
    "standardization": "<DATABRICKS_JOB_ID_STANDARDIZATION>",
    "canonical": "<DATABRICKS_JOB_ID_CANONICAL>",
    "dq": "<DATABRICKS_JOB_ID_DQ>",
    "ingress_hcp": "<DATABRICKS_JOB_ID_INGRESS_HCP>",
    "ingress_hco": "<DATABRICKS_JOB_ID_INGRESS_HCO>",
    "egress_hcp": "<DATABRICKS_JOB_ID_EGRESS_HCP>",
    "egress_hco": "<DATABRICKS_JOB_ID_EGRESS_HCO>",
}

default_args = {
    "owner": "hmdm-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="hmdm_iqvia_hcp_hco_pipeline",
    description="Healthcare MDM: IQVIA Source -> Raw -> Landing -> Staging -> MDM Ingress -> MDM Egress",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # daily at 03:00 - adjust to match the real IQVIA feed cadence
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["healthcare-mdm", "iqvia", "informatica"],
) as dag:

    def databricks_task(task_id: str, job_key: str, notebook_params: dict | None = None):
        """Build one DatabricksRunNowOperator task for a given pipeline stage."""
        return DatabricksRunNowOperator(
            task_id=task_id,
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=JOB_IDS[job_key],
            notebook_params=notebook_params or {},
        )

    t_ingestion = databricks_task("source_to_raw_ingestion", "ingestion")
    t_standardization = databricks_task("raw_to_land_standardization", "standardization")
    t_canonical = databricks_task("canonical_standardization", "canonical")
    t_dq = databricks_task("land_to_stage_dq", "dq")
    t_ingress_hcp = databricks_task("mdm_ingress_hcp", "ingress_hcp", {"entity_type": "HCP"})
    t_ingress_hco = databricks_task("mdm_ingress_hco", "ingress_hco", {"entity_type": "HCO"})
    t_egress_hcp = databricks_task("mdm_egress_hcp_master", "egress_hcp", {"entity_type": "HCP"})
    t_egress_hco = databricks_task("mdm_egress_hco_master", "egress_hco", {"entity_type": "HCO"})

    # Dependency chain matches get_batch_status_filter() in
    # src/core/runtime_config.py: raw_ingestion -> stdz -> canonical -> dq
    # -> ingress -> egress. HCP and HCO ingress/egress run in parallel
    # since they are independent entities once Staging is ready.
    t_ingestion >> t_standardization >> t_canonical >> t_dq
    t_dq >> [t_ingress_hcp, t_ingress_hco]
    t_ingress_hcp >> t_egress_hcp
    t_ingress_hco >> t_egress_hco
