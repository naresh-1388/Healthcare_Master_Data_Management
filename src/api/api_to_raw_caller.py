"""
API to RAW Caller - Production Pattern
=======================================
Following download_api.py pattern with AWS Secrets Manager.

This module:
1. Reads entity IDs (HCP/HCO) from control table
2. Gets Lambda URL and API key from AWS Secrets Manager
3. Calls Lambda API for each entity
4. Transforms responses to RAW schema
5. Writes to RAW Delta tables
6. Logs all execution details

NO HARDCODED CREDENTIALS OR URLS!
All sensitive data comes from AWS Secrets Manager.
"""

import json
import time
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
import requests

# Import common utilities
# runtime_config now handles Serverless properly (no new SparkSession creation)
try:
    from ..core.runtime_config import catalog, env, util_schema
    from ..core.logging_utils import log_event_detail, logger
    from ..core.data_io import send_email
except ImportError:
    from core.runtime_config import catalog, env, util_schema
    from core.logging_utils import log_event_detail, logger
    from core.data_io import send_email

# Constants
RETRY_COUNT = 5
RETRY_BACKOFF = 2  # seconds
REQUEST_TIMEOUT = 30  # seconds
CONTROL_TABLE = f"{util_schema}.ctl_api_entity_list"  # util_schema already includes catalog

# AWS Secrets Manager configuration
REGION_NAME = os.getenv("region", "us-east-1")
SECRET_NAME = os.getenv("secret_name", "healthcare-mdm/dev/api-snowflake")


class APICallerError(Exception):
    """Custom exception for API caller errors."""
    pass


def get_secret(
    secret_name: str,
    region_name: str = REGION_NAME,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Retrieve credentials from AWS Secrets Manager.
    
    This follows the exact pattern from download_api.py.
    
    Args:
        secret_name: AWS Secrets Manager secret name
        region_name: AWS region
        aws_access_key_id: Optional AWS access key (for Serverless)
        aws_secret_access_key: Optional AWS secret key (for Serverless)
        
    Returns:
        dict: Secret values as dictionary
        
    Raises:
        APICallerError: If secret cannot be retrieved
    """
    # Create client with explicit credentials if provided (for Databricks Serverless)
    if aws_access_key_id and aws_secret_access_key:
        client = boto3.client(
            "secretsmanager",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    else:
        client = boto3.client("secretsmanager", region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except Exception as exc:
        raise APICallerError(f"Error retrieving secret: {str(exc)}") from exc
    
    if "SecretString" in response:
        return json.loads(response["SecretString"])
    
    return response["SecretBinary"]


def get_lambda_credentials(
    secret_name: str = SECRET_NAME,
    region_name: str = REGION_NAME,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None
) -> tuple:
    """
    Get Lambda URL and API key from AWS Secrets Manager.
    
    Expected secret JSON structure:
    {
        "lambda_url": "https://...",
        "lambda_api_key": "..."
    }
    
    Args:
        secret_name: AWS Secrets Manager secret name
        region_name: AWS region
        aws_access_key_id: Optional AWS access key (for Serverless)
        aws_secret_access_key: Optional AWS secret key (for Serverless)
        
    Returns:
        tuple: (lambda_url, api_key)
        
    Raises:
        APICallerError: If credentials cannot be retrieved
    """
    try:
        secret = get_secret(secret_name, region_name, aws_access_key_id, aws_secret_access_key)
        lambda_url = secret.get("lambda_url")
        api_key = secret.get("lambda_api_key")
        
        if not lambda_url or not api_key:
            raise APICallerError(
                "Lambda URL or API key missing in secret. "
                f"Expected keys: lambda_url, lambda_api_key"
            )
        
        logger.info(f"Successfully retrieved Lambda credentials from AWS Secrets Manager: {secret_name}")
        return lambda_url, api_key
        
    except Exception as e:
        raise APICallerError(f"Failed to retrieve Lambda credentials: {str(e)}")


def get_pending_entities(entity_types: List[str] = None, max_records: int = None, spark_session: Optional[Any] = None):
    """
    Read pending entity IDs from control table.
    
    Args:
        entity_types: List of entity types to process (e.g., ["HCP", "HCO"])
        max_records: Maximum number of records to process (for testing)
        spark_session: Optional SparkSession (for Databricks Serverless catalog fix)
        
    Returns:
        DataFrame with columns: entity_id, entity_type, country_code
    """
    spark = spark_session if spark_session else SparkSession.builder.getOrCreate()
    
    # CRITICAL FIX for Databricks Serverless:
    # Ensure we're using HMDM_DEV catalog, not spark_catalog
    try:
        current_catalog = spark.catalog.currentCatalog()
        if current_catalog != catalog:
            logger.info(f"Switching from {current_catalog} to {catalog}")
            spark.sql(f"USE CATALOG {catalog}")
    except Exception as e:
        logger.warning(f"Could not switch catalog: {e}")
    
    # Check if control table exists (SERVERLESS-SAFE: using SQL instead of catalog API)
    try:
        # Extract just the schema name (util_schema is already "catalog.schema")
        schema_only = util_schema.split('.')[-1]  # Gets "util" from "hmdm_dev.util"
        table_exists = spark.sql(f"SHOW TABLES IN {schema_only} LIKE 'ctl_api_entity_list'").count() > 0
    except Exception:
        table_exists = False
    
    if not table_exists:
        logger.warning(f"Control table does not exist: {CONTROL_TABLE}")
        logger.info("Creating control table with sample data...")
        
        # Create sample data for testing
        sample_data = [
            ("W12345678", "HCP", "ACTIVE", "NL", 1),
            ("W87654321", "HCP", "ACTIVE", "BE", 1),
            ("W11111111", "HCO", "ACTIVE", "NL", 1),
            ("W22222222", "HCO", "ACTIVE", "BE", 2),
        ]
        
        df_sample = spark.createDataFrame(
            sample_data,
            ["entity_id", "entity_type", "status", "country_code", "priority"]
        )
        
        df_sample = df_sample.withColumn("create_date", F.current_timestamp()) \
                             .withColumn("update_date", F.current_timestamp()) \
                             .withColumn("last_processed_date", F.lit(None).cast("timestamp"))
        
        df_sample.write.format("delta").mode("overwrite").saveAsTable(CONTROL_TABLE)
        logger.info(f"Created control table: {CONTROL_TABLE}")
    
    # Read pending entities
    query = f"""
        SELECT 
            entity_id,
            entity_type,
            country_code,
            priority
        FROM {CONTROL_TABLE}
        WHERE status = 'ACTIVE'
    """
    
    if entity_types:
        entity_types_str = "', '".join(entity_types)
        query += f" AND entity_type IN ('{entity_types_str}')"
    
    query += " ORDER BY priority, entity_id"
    
    if max_records:
        query += f" LIMIT {max_records}"
    
    df = spark.sql(query)
    
    count = df.count()
    logger.info(f"Found {count} pending entities to process")
    
    return df


def make_lambda_call(
    lambda_url: str,
    api_key: str,
    entity_id: str,
    entity_type: str,
    retry_count: int = RETRY_COUNT
) -> Dict[str, Any]:
    """
    Call Lambda API with retry mechanism.
    Following download_api.py retry pattern.
    
    Args:
        lambda_url: Lambda function URL
        api_key: API authentication key
        entity_id: Entity ID (IQVIA ID)
        entity_type: HCP or HCO
        retry_count: Number of retry attempts
        
    Returns:
        JSON response from Lambda
        
    Raises:
        APICallerError: If all retries fail
    """
    import requests
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "mdmEntityType": entity_type,
        "iqviaId": entity_id
    }
    
    for attempt in range(retry_count):
        try:
            logger.info(
                f"Calling Lambda for {entity_type} ID: {entity_id} "
                f"(attempt {attempt + 1}/{retry_count})"
            )
            
            response = requests.post(
                lambda_url,
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                logger.info(f"Lambda call successful for {entity_id}")
                return response.json()
            else:
                logger.warning(
                    f"Lambda returned {response.status_code} for {entity_id}: "
                    f"{response.text[:200]}"
                )
                
        except requests.exceptions.Timeout:
            logger.warning(f"Lambda call timeout for {entity_id}")
        except Exception as e:
            logger.warning(f"Lambda call failed: {str(e)}")
        
        # Exponential backoff before retry
        if attempt < retry_count - 1:
            sleep_time = RETRY_BACKOFF ** attempt
            logger.info(f"Retrying after {sleep_time} seconds...")
            time.sleep(sleep_time)
    
    raise APICallerError(
        f"Lambda call failed after {retry_count} retries for {entity_id}"
    )


def validate_lambda_response(response_json: Dict[str, Any], entity_type: str) -> bool:
    """
    Validate Lambda response structure.
    Following download_api.py validation pattern.
    
    Args:
        response_json: Lambda response
        entity_type: HCP or HCO
        
    Returns:
        True if valid, False otherwise
    """
    if not response_json:
        logger.warning("Empty response")
        return False
    
    # Check required fields based on entity type
    if entity_type == "HCP":
        required_fields = ["First Name", "Last Name"]
    elif entity_type == "HCO":
        required_fields = ["Organization Name"]
    else:
        logger.warning(f"Unknown entity type: {entity_type}")
        return False
    
    for field in required_fields:
        if field not in response_json:
            logger.warning(f"Missing required field: {field}")
            return False
    
    return True


def transform_to_raw_schema(
    entity_id: str,
    entity_type: str,
    response_json: Dict[str, Any],
    country_code: str
) -> Dict[str, Any]:
    """
    Transform Lambda JSON to RAW table schema.
    Following download_api.py build_records pattern.
    
    Args:
        entity_id: Entity ID
        entity_type: HCP or HCO
        response_json: Lambda response
        country_code: Country code from control table
        
    Returns:
        Dictionary with RAW schema fields
    """
    current_timestamp = datetime.now().isoformat()
    
    # Base record (common for all entity types)
    record = {
        "iqvia_id": entity_id,
        "mdm_entity_type": entity_type,
        "country_code": country_code,
        "response_json": json.dumps(response_json),  # Full JSON as string
        "create_date": current_timestamp,
        "load_date": current_timestamp,
        "source_name": "IQVIA_API",
        "batch_id": f"API_{entity_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    }
    
    # Add entity-specific fields based on entity type
    if entity_type == "HCP":
        record.update({
            "title": response_json.get("Title", ""),
            "first_name": response_json.get("First Name", ""),
            "middle_name": response_json.get("Middle Name", ""),
            "last_name": response_json.get("Last Name", ""),
            "full_name": response_json.get("Full Name", ""),
            "gender": response_json.get("Gender", ""),
            "professional_type": response_json.get("Professional Type", "")
        })
        
    elif entity_type == "HCO":
        record.update({
            "organization_name": response_json.get("Organization Name", ""),
            "organization_type": response_json.get("Organization Type", "")
        })
    
    return record


def run_api_to_raw(
    entity_types: List[str] = None,
    max_records: int = None,
    secret_name: str = None,
    region_name: str = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    lambda_url: Optional[str] = None,
    lambda_api_key: Optional[str] = None
) -> Dict[str, int]:
    """
    Main orchestration function.
    Following download_api.py run_api_to_src pattern.
    
    Args:
        entity_types: List of entity types to process (default: ["HCP", "HCO"])
        max_records: Maximum records to process (for testing)
        secret_name: AWS Secrets Manager secret name (default: from env or SECRET_NAME)
        region_name: AWS region (default: from env or REGION_NAME)
        aws_access_key_id: Optional AWS access key (for Databricks Serverless)
        aws_secret_access_key: Optional AWS secret key (for Databricks Serverless)
        lambda_url: Optional Lambda URL (if provided, skips AWS Secrets Manager lookup)
        lambda_api_key: Optional Lambda API key (if provided, skips AWS Secrets Manager lookup)
        
    Returns:
        Dictionary with processing statistics
    """
    if entity_types is None:
        entity_types = ["HCP", "HCO"]
    
    logger.info(f"Starting API to RAW ingestion for entity types: {entity_types}")
    start_time = datetime.now()
    
    stats = {
        "total_processed": 0,
        "total_success": 0,
        "total_failed": 0,
        "hcp_count": 0,
        "hco_count": 0
    }
    
    try:
        # Step 1: Get Lambda credentials (use provided or fetch from AWS Secrets Manager)
        if lambda_url and lambda_api_key:
            api_key = lambda_api_key
            logger.info("Using provided Lambda credentials")
        else:
            lambda_url, api_key = get_lambda_credentials(
                secret_name=secret_name or SECRET_NAME,
                region_name=region_name or REGION_NAME,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
        
        # Step 2: Get pending entities from control table
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"USE CATALOG {catalog}")  # Ensure catalog is set
        entities_df = get_pending_entities(entity_types, max_records, spark_session=spark)
        
        if entities_df.count() == 0:
            logger.warning("No entities found to process")
            return stats
        
        # Step 3: Process each entity
        entities = entities_df.collect()
        
        for entity_row in entities:
            entity_id = entity_row["entity_id"]
            entity_type = entity_row["entity_type"]
            country_code = entity_row["country_code"]
            
            stats["total_processed"] += 1
            
            try:
                # Step 3a: Call Lambda API
                response_json = make_lambda_call(
                    lambda_url, api_key, entity_id, entity_type
                )
                
                # Step 3b: Validate response
                if not validate_lambda_response(response_json, entity_type):
                    logger.warning(f"Invalid response for {entity_id}, skipping")
                    stats["total_failed"] += 1
                    continue
                
                # Step 3c: Transform to RAW schema
                raw_record = transform_to_raw_schema(
                    entity_id, entity_type, response_json, country_code
                )
                
                # Step 3d: Write to RAW Delta table
                spark = SparkSession.builder.getOrCreate()
                df = spark.createDataFrame([raw_record])
                
                # Determine target table based on entity type
                if entity_type == "HCP":
                    target_table = f"{catalog}.raw.hcp_api_data"
                    stats["hcp_count"] += 1
                elif entity_type == "HCO":
                    target_table = f"{catalog}.raw.hco_api_data"
                    stats["hco_count"] += 1
                else:
                    logger.warning(f"Unknown entity type: {entity_type}")
                    stats["total_failed"] += 1
                    continue
                
                # Write to Delta
                df.write.format("delta").mode("append").saveAsTable(target_table)
                
                logger.info(f"Successfully wrote {entity_id} to {target_table}")
                stats["total_success"] += 1
                
            except Exception as e:
                logger.error(f"Failed to process {entity_id}: {str(e)}")
                stats["total_failed"] += 1
                continue
        
        # Step 4: Log execution summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"API to RAW ingestion completed in {duration:.2f} seconds")
        logger.info(f"Statistics: {stats}")
        
        # Log to audit table
        log_event_detail(
            "API_TO_RAW_INGESTION",
            "SUCCESS",
            f"Processed {stats['total_processed']} entities, "
            f"{stats['total_success']} success, {stats['total_failed']} failed",
            "",
            "api_to_raw_caller",
            "IQVIA_LAMBDA",
            "",
            "API_TO_RAW",
            start_time,
            "",
            ""
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"API to RAW ingestion failed: {str(e)}")
        
        # Log failure
        log_event_detail(
            "API_TO_RAW_INGESTION",
            "FAILED",
            str(e),
            "",
            "api_to_raw_caller",
            "IQVIA_LAMBDA",
            "",
            "API_TO_RAW",
            start_time,
            "",
            ""
        )
        
        raise APICallerError(f"Pipeline failed: {str(e)}")
