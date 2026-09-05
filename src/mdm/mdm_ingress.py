"""
MDM Ingress preparation for HCO.

Source of truth:
    Data Info.xlsx -> Stg_MDM_Ingress-HCO

Purpose:
    Transform validated HCO staging records into the structure
    required by Informatica MDM HCO ingress.

Transformation logic in the mapping workbook is "Pass Through"
for all currently supplied HCO ingress mappings.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


try:
    from ..core.logging_utils import logger, log_event_detail
    from ..core.runtime_config import (
        batch_log_tbl,
        catalog,
        get_batch_status_filter,
        get_notebook_run_url,
        job_id,
        run_id,
        cluster_id,
        update_batch_log_tbl,
    )
except ImportError:
    from core.logging_utils import logger, log_event_detail
    from core.runtime_config import (
        batch_log_tbl,
        catalog,
        get_batch_status_filter,
        get_notebook_run_url,
        job_id,
        run_id,
        cluster_id,
        update_batch_log_tbl,
    )


spark = SparkSession.builder.getOrCreate()

MODULE_NAME = "MDM_INGRESS"


class GracefulExit(Exception):
    """Expected pipeline termination."""


class IngressProcessingError(Exception):
    """MDM ingress processing failure."""


# ---------------------------------------------------------------------
# Exact HCO mapping from Data Info.xlsx
# ---------------------------------------------------------------------

HCO_INGRESS_MAPPING = [
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Population_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "populationName",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "HCO_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "HCO_Subtype",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_companyType",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Country",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_countryOfIncorporation",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Bed_Count",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_infa360_bedCount",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Resident_Count",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_infa360_residentCount",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Website",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_infa360_website",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "HCO_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_infa360_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_hco_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Transparency_Reporting_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_transparency_reporting_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Official_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_official_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Parent_Organization_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_parent_organization_name",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Teaching_Hospital_Flag",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_TeachingHospitalFlag",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Profit_Flag",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_ProfitFlag",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Accept_Medicare",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_AcceptMedicare",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "E_Medical_Record",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_EMedicalRecord",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Pay_Perform",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_PayPerform",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "E_Prescribe",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_EPrescribe",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Formulary",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_Formulary",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Activation_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_ActivationDate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Accept_Medicaid",
        "data_type": "boolean",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_AcceptMedicaid",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Ownership_Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_ownership_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Source_Created_Date",
        "data_type": "timestamp",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_source_createdate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Source_Updated_Date",
        "data_type": "timestamp",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_source_updatedate",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": "eda_de_Parexel_mdm_lake.Par_lake_stg_hco_name",
        "src_attribute": "Third_Party_ID",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/C360org.X_name"
        ),
        "tgt_attribute": "HCO_X_third_party_id",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Identification
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Identifier_Value",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "altValue",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "IdentifierStatus",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Identifier_Issuer",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "X_infa360_identifierIssuer",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Issuing_Country",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "X_infa360_issuingCountry",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Issuing_State",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "X_infa360_issuingState",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Activation_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "X_activation_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Expiration_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "X_expiration_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_identification"
        ),
        "src_attribute": "Source_FK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.AlternateIdentifier"
        ),
        "tgt_attribute": "AlternateIdentifier_parentId",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Phone
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Phone_PK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "sourcePKey",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Primary_Phone",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_primary_phone",
        "tgt_data_type": "integer",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Phone_Usage_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_usage_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Phone_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Phone_Number",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_number",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Phone_Number_Extension",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_number_extension",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "ISO",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_iso",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Effective_Start_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_effective_start_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Effective_End_Date",
        "data_type": "date",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_effective_end_date",
        "tgt_data_type": "date/time",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_phone"
        ),
        "src_attribute": "Source_FK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_phone"
        ),
        "tgt_attribute": "X_phone_parentId",
        "tgt_data_type": "string",
    },

    # -------------------------------------------------------------
    # HCO Specialty
    # -------------------------------------------------------------

    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Specialty_PK",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "sourcePKey",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Specialty_Rank",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_infa360_rank",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_infa360_Specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Specialty_Type",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_specialty_type",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Status",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_specialty_status",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Specialty_Source",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_specialty_source",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Global_Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_global_specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Group_Specialty",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "X_group_specialty",
        "tgt_data_type": "string",
    },
    {
        "src_tbl_nm": (
            "eda_de_Parexel_mdm_lake."
            "Par_lake_stg_hco_specialty"
        ),
        "src_attribute": "Source_Name",
        "data_type": "string",
        "transformation": "Pass Through",
        "tgt_tbl_nm": (
            "Home/Cust 360/Business Entity/HCO/"
            "C360org.X_infa360_Specialty"
        ),
        "tgt_attribute": "sourceSystem",
        "tgt_data_type": "string",
    },
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def read_parameters(
    args: Optional[List[str]] = None,
) -> Dict[str, str]:

    if args is None:
        args = sys.argv[1:]

    if len(args) < 2:
        raise ValueError(
            "Expected arguments: "
            "<source_identifier> <source_system_name>"
        )

    return {
        "source_identifier": args[0],
        "source_system_name": args[1],
    }


def _table_aliases() -> Dict[str, str]:
    """Load optional logical->physical table aliases from runtime config."""
    raw = os.getenv("HEALTHCARE_MDM_TABLE_ALIASES", "")
    if not raw:
        return {}
    import json
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("HEALTHCARE_MDM_TABLE_ALIASES must be a JSON object")
    return {str(k): str(v) for k, v in parsed.items()}


def qualify_table(table_name: str) -> str:
    """Resolve a workbook logical table through runtime aliases only.

    The mapping workbook does not provide physical tables for the current
    environment, so no physical name is fabricated here.
    """
    aliases = _table_aliases()
    if table_name in aliases:
        return aliases[table_name]
    if table_name.startswith(f"{catalog}."):
        return table_name
    return table_name


def read_source_table(
    table_name: str,
    batch_condition: str,
) -> DataFrame:

    qualified_name = qualify_table(
        table_name
    )

    logger.info(
        f"Reading ingress source: {qualified_name}"
    )

    return spark.sql(
        f"""
        SELECT *
        FROM {qualified_name}
        WHERE {batch_condition}
        """
    )


# ---------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------

def transform_source(
    source_df: DataFrame,
    mappings: List[dict],
) -> DataFrame:
    """
    Apply the exact Pass Through mappings.

    Target datatype conversions are applied only where the workbook
    explicitly specifies the target datatype.
    """

    expressions = []

    for mapping in mappings:

        source_column = mapping["src_attribute"]
        target_column = mapping["tgt_attribute"]
        target_type = mapping["tgt_data_type"]

        expression = get_column(
            source_df,
            source_column,
        )

        if target_type == "integer":
            expression = expression.cast("integer")

        elif target_type == "date/time":
            expression = expression.cast("timestamp")

        elif target_type == "date":
            expression = expression.cast("date")

        elif target_type == "string":
            expression = expression.cast("string")

        expressions.append(
            expression.alias(target_column)
        )

    return source_df.select(
        *expressions
    )


# ---------------------------------------------------------------------
# Write / outbound preparation
# ---------------------------------------------------------------------

def prepare_hco_ingress(
    batch_condition: str,
) -> Dict[str, DataFrame]:
    """
    Prepare one DataFrame per Informatica MDM target object.

    This preserves the target-object grouping present in Excel.
    """

    target_groups = {}

    for mapping in HCO_INGRESS_MAPPING:

        source_table = mapping["src_tbl_nm"]
        target_object = mapping["tgt_tbl_nm"]

        target_groups.setdefault(
            target_object,
            [],
        ).append(mapping)

    result = {}

    for target_object, mappings in target_groups.items():

        source_tables = {
            mapping["src_tbl_nm"]
            for mapping in mappings
        }

        if len(source_tables) != 1:
            raise IngressProcessingError(
                f"Multiple source tables mapped to target "
                f"{target_object}. Explicit source-to-target "
                f"join logic is not supplied."
            )

        source_table = next(
            iter(source_tables)
        )

        source_df = read_source_table(
            source_table,
            batch_condition,
        )

        result[target_object] = transform_source(
            source_df,
            mappings,
        )

    return result


# ---------------------------------------------------------------------
# Persist prepared ingress datasets
# ---------------------------------------------------------------------

def write_prepared_ingress(
    target_object: str,
    df: DataFrame,
    output_table: str,
) -> int:
    """
    Persist the prepared ingress dataset.

    The physical Snowflake/Delta target table is supplied by runtime
    configuration rather than invented from the Informatica object
    path.
    """

    count = df.count()

    if count == 0:
        logger.info(
            f"No records for MDM target object "
            f"{target_object}"
        )
        return 0

    (
        df.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(output_table)
    )

    logger.info(
        f"Wrote {count} records for "
        f"{target_object} -> {output_table}"
    )

    return count


# ---------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------

def process_hco_ingress(
    source_identifier: str,
    source_system_name: str,
    output_table_map: Dict[str, str],
) -> None:

    start_time = datetime.now()
    run_url = get_notebook_run_url()

    batch_status_condition = get_batch_status_filter(
        "ingress",
        source_system_name,
    )

    batch_rows = spark.sql(
        f"""
        SELECT DISTINCT batch_id
        FROM {batch_log_tbl}
        WHERE {batch_status_condition}
        ORDER BY batch_id
        """
    ).collect()

    batch_ids = [
        row["batch_id"]
        for row in batch_rows
        if row["batch_id"] is not None
    ]

    if not batch_ids:

        log_event_detail(
            f"MDM Ingress - HCO",
            "Passed",
            "No batches ready for MDM ingress.",
            run_url,
            source_identifier,
            source_system_name,
            job_id,
            MODULE_NAME,
            start_time,
            cluster_id,
            run_id,
        )

        raise GracefulExit(
            "No batches ready for MDM ingress."
        )

    batch_condition = (
        "batch_id IN ("
        + ", ".join(
            f"'{str(batch_id).replace(chr(39), chr(39) * 2)}'"
            for batch_id in batch_ids
        )
        + ")"
    )

    prepared_datasets = prepare_hco_ingress(
        batch_condition
    )

    total_records = 0

    for target_object, df in prepared_datasets.items():

        if target_object not in output_table_map:
            raise IngressProcessingError(
                f"No physical output table configured for "
                f"Informatica target object: {target_object}"
            )

        total_records += write_prepared_ingress(
            target_object,
            df,
            output_table_map[target_object],
        )

    update_batch_log_tbl(
        "ingress",
        "Y",
        batch_condition,
        source_system_name,
    )

    log_event_detail(
        "MDM Ingress - HCO",
        "Passed",
        f"Successfully prepared {total_records} records.",
        run_url,
        source_identifier,
        source_system_name,
        job_id,
        MODULE_NAME,
        start_time,
        cluster_id,
        run_id,
    )


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

def main() -> None:

    parameters = read_parameters()

    # IMPORTANT:
    # Physical output table names are not present in the supplied
    # Stg_MDM_Ingress-HCO mapping. Therefore they must be provided
    # through runtime/configuration rather than invented here.
    #
    # Example structure expected at runtime:
    #
    # {
    #   "Home/Cust 360/Business Entity/HCO/C360org.X_name":
    #       "<configured physical table>",
    #   "Home/Cust 360/Business Entity/HCO/C360org.AlternateIdentifier":
    #       "<configured physical table>",
    #   "Home/Cust 360/Business Entity/HCO/C360org.X_phone":
    #       "<configured physical table>",
    #   "Home/Cust 360/Business Entity/HCO/C360org.X_infa360_Specialty":
    #       "<configured physical table>"
    # }
    #
    # Do NOT hardcode physical tables here without a source/config.

    output_table_map: Dict[str, str] = {}

    # Physical Snowflake/Databricks target table names are not supplied by
    # the workbook.  Accept them only from runtime configuration.
    mapping_json = os.getenv("HEALTHCARE_MDM_HCO_INGRESS_TABLE_MAP", "")
    if mapping_json:
        import json
        parsed = json.loads(mapping_json)
        if not isinstance(parsed, dict):
            raise ValueError("HEALTHCARE_MDM_HCO_INGRESS_TABLE_MAP must be a JSON object")
        output_table_map.update({str(k): str(v) for k, v in parsed.items()})

    try:

        process_hco_ingress(
            parameters["source_identifier"],
            parameters["source_system_name"],
            output_table_map,
        )

    except GracefulExit as exc:

        logger.info(
            str(exc)
        )

    except Exception as exc:

        logger.exception(
            "HCO MDM ingress failed."
        )

        try:
            update_batch_log_tbl(
                "ingress",
                "N",
                None,
                parameters["source_system_name"],
            )
        except Exception:
            logger.exception(
                "Unable to update ingress batch status."
            )

        raise IngressProcessingError(
            "HCO MDM ingress processing failed."
        ) from exc


if __name__ == "__main__":
    main()

# ============================================================================
# USER CONFIGURATION - MDM / SNOWFLAKE
# ============================================================================
# 1) Enter the physical Snowflake source/target database and schema names only
#    in the runtime/config mapping where the project provides them.
# 2) The Excel mapping supplies MDM object paths and attributes; it does NOT
#    supply physical Snowflake table names for every object. Do not invent them.
# 3) Informatica MDM connection credentials must be stored in the approved
#    runtime secret store, never hardcoded here.
# ============================================================================

