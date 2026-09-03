"""
MDM HUB Egress - HCP

Source of truth:
    Data Info.xlsx
    Sheet: MDM_HUB_Egress-HCP_Master

The workbook defines source MDM attributes and target HUB attributes.
No additional business transformations are introduced here.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from ..core.logging_utils import logger
except ImportError:
    from core.logging_utils import logger


spark = SparkSession.builder.getOrCreate()


MODULE_NAME = "MDM_HUB_EGRESS"


class GracefulExit(Exception):
    """Expected pipeline termination."""


class EgressProcessingError(Exception):
    """MDM HUB egress processing error."""


# ---------------------------------------------------------------------
# Exact mapping from Data Info.xlsx
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class EgressMapping:
    source_object: str
    source_attribute: str
    target_table: str
    target_attribute: str


HCP_EGRESS_MAPPING: List[EgressMapping] = [

    # ================================================================
    # Specialty
    # ================================================================

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_SpecialtyType",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Specialty",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_SpecialtyClass",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Specialty_Type",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_SpecialtyRank",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Specialty_Rank",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_taxonomyName",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Taxonomy_Name",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_group",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Group",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_taxonomy_code",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Taxonomy_Code",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_infa360_subClassification",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Sub_Classification",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "X_specialty_status",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Specialty_Status",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_Specialty",
        "O_Load_Date",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_specialty",
        "Load_Date",
    ),

    # ================================================================
    # Alternate Name
    # ================================================================

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "alternateNameType",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Name_Type",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "AlternateName",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Alternate_Name",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "X_alternate_name_status",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Alternate_Name_Status",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "effectiveStartDate",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Effective_Start_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "effectiveEndDate",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Effective_End_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.AlternateName",
        "O_Load_Date",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_alternatename",
        "Load_Date",
    ),

    # ================================================================
    # Therapeutic Area
    # ================================================================

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_TherapeuticArea",
        "X_infa360_TherapeuticArea_parentId",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_therapeuticarea",
        "Global_HCP_ID",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_TherapeuticArea",
        "X_infa360_activeIndicator",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_therapeuticarea",
        "Active_Indicator",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_TherapeuticArea",
        "X_infa360_therapeuticArea",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_therapeuticarea",
        "Therapeutic_Area",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_TherapeuticArea",
        "O_Load_Date",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_therapeuticarea",
        "Load_Date",
    ),

    # ================================================================
    # License
    # ================================================================

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_License_parentId",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Global_HCP_ID",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "sourcePKey",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Source_PK",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_LicenseType",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "License_Type",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_LicenseNumber",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "License_Number",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_country",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Country",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_state",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "State",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_sample_elig",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "License_Sample_Eligibility",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_sampleability_overall",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Sampleability_Overall",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_sampleability_lastreceived_date",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Sampleability_Last_Received_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_sampleability_fed_sanctions_date",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Sampleability_Fed_Sanctions_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_sampleability_desigstatus",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Sampleability_Designation_Status",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_issueDate",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Issue_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_expiryDate",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Expiry_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_degree",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Degree",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_adjLic_expdate",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Adj_License_Exp_Date",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_AdjCode",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Adj_Code",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_AdjCodesDescriptions",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "Adj_Codes_Descriptions",
    ),

    EgressMapping(
        "Home/Cust 360/Business Entity/HCP/"
        "C360person.X_infa360_License",
        "X_infa360_status",
        "eda_de_Parexel_mdm_hub/TABLE/"
        "Par_hub_master_hcp_license",
        "License_Status",
    ),
]


# ---------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------

def mappings_by_source() -> Dict[str, List[EgressMapping]]:
    result: Dict[str, List[EgressMapping]] = {}

    for mapping in HCP_EGRESS_MAPPING:
        result.setdefault(
            mapping.source_object,
            [],
        ).append(mapping)

    return result


def mappings_by_target() -> Dict[str, List[EgressMapping]]:
    result: Dict[str, List[EgressMapping]] = {}

    for mapping in HCP_EGRESS_MAPPING:
        result.setdefault(
            mapping.target_table,
            [],
        ).append(mapping)

    return result


def resolve_column(
    df: DataFrame,
    column_name: str,
):
    columns = {
        column.lower(): column
        for column in df.columns
    }

    actual_name = columns.get(
        column_name.lower()
    )

    if actual_name is None:
        raise EgressProcessingError(
            f"MDM source column '{column_name}' "
            f"does not exist. "
            f"Available columns: {df.columns}"
        )

    return F.col(actual_name)


# ---------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------

def transform_for_target(
    source_df: DataFrame,
    mappings: List[EgressMapping],
) -> DataFrame:

    expressions = []

    for mapping in mappings:

        expressions.append(
            resolve_column(
                source_df,
                mapping.source_attribute,
            ).alias(
                mapping.target_attribute
            )
        )

    return source_df.select(
        *expressions
    )


# ---------------------------------------------------------------------
# Process one MDM source object
# ---------------------------------------------------------------------

def process_source_object(
    source_object: str,
    source_table: str,
) -> Dict[str, DataFrame]:

    mappings = mappings_by_source().get(
        source_object,
        [],
    )

    if not mappings:
        raise EgressProcessingError(
            f"No HCP egress mapping found for "
            f"{source_object}"
        )

    source_df = spark.table(
        source_table
    )

    output: Dict[str, DataFrame] = {}

    target_groups: Dict[
        str,
        List[EgressMapping]
    ] = {}

    for mapping in mappings:
        target_groups.setdefault(
            mapping.target_table,
            [],
        ).append(mapping)

    for target_table, target_mappings in target_groups.items():

        output[target_table] = transform_for_target(
            source_df,
            target_mappings,
        )

    return output


# ---------------------------------------------------------------------
# Write target HUB table
# ---------------------------------------------------------------------

def write_target(
    df: DataFrame,
    target_table: str,
) -> int:

    record_count = df.count()

    if record_count == 0:
        return 0

    (
        df.write
        .mode("append")
        .option(
            "mergeSchema",
            "true",
        )
        .saveAsTable(
            target_table
        )
    )

    logger.info(
        f"Loaded {record_count} records "
        f"into {target_table}"
    )

    return record_count


# ---------------------------------------------------------------------
# Main HCP egress process
# ---------------------------------------------------------------------

def process_hcp_egress(
    source_table_map: Dict[str, str],
    target_table_map: Dict[str, str],
) -> int:

    total_records = 0

    source_groups = mappings_by_source()

    for source_object, mappings in source_groups.items():

        if source_object not in source_table_map:
            raise EgressProcessingError(
                f"Physical source table is not configured "
                f"for MDM object: {source_object}"
            )

        source_table = source_table_map[
            source_object
        ]

        logger.info(
            f"Processing MDM source object: "
            f"{source_object}"
        )

        target_data = process_source_object(
            source_object,
            source_table,
        )

        for target_object, dataframe in target_data.items():

            if target_object not in target_table_map:
                raise EgressProcessingError(
                    f"Physical HUB target table is not "
                    f"configured for: {target_object}"
                )

            physical_target = target_table_map[
                target_object
            ]

            total_records += write_target(
                dataframe,
                physical_target,
            )

    return total_records


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

def main() -> None:

    logger.info(
        "Starting HCP MDM HUB egress."
    )

    # Physical source/target table names are intentionally not
    # fabricated here because the Excel egress sheet supplies
    # Informatica object paths, not physical runtime table names.
    #
    # Populate these from the project's runtime/configuration source.

    source_table_map: Dict[str, str] = {}

    target_table_map: Dict[str, str] = {}

    try:

        total = process_hcp_egress(
            source_table_map=source_table_map,
            target_table_map=target_table_map,
        )

        logger.info(
            f"HCP MDM HUB egress completed. "
            f"Total records: {total}"
        )

    except GracefulExit as exc:

        logger.info(str(exc))

    except Exception as exc:

        logger.exception(
            "HCP MDM HUB egress failed."
        )

        raise EgressProcessingError(
            "HCP MDM HUB egress processing failed."
        ) from exc


if __name__ == "__main__":
    main()

# ============================================================================
# USER CONFIGURATION - MDM / SNOWFLAKE
# ============================================================================
# 1) Configure physical Snowflake source/target database/schema/table names only
#    where the project specification supplies or you explicitly provide them.
# 2) The MDM object paths in Data Info.xlsx are logical target paths; do not
#    replace them with invented physical table names.
# 3) Informatica MDM credentials/endpoints belong in the runtime secret store.
# ============================================================================

