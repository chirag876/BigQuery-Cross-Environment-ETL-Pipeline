from typing import List, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import core.constants as const
from core.bigquery.client import BQ
from core.database.billing_etl_db import BillingETLDB
from core.database.database_class import DB
from core.utility import logger, return_type, sa_utils
from google.api_core.exceptions import Forbidden
from time import sleep

def create_single_dataset_with_verification(
    bq_client: BQ, project_id: str, dataset_suffix: str
) -> Tuple[str, str]:
    """Creates a single dataset with verification and retry logic"""
    max_creation_attempts = const.MAX_DATASET_CREATION_ATTEMPTS
    for attempt in range(max_creation_attempts):
        dataset_id = f"{project_id}.{dataset_suffix}"

        try:
            dataset = bq_client.create_new_dataset(
                dataset_id=dataset_id, location="US")

            if dataset is None:
                logger.warning_log(
                    f"{'Retrying creation...' if attempt < max_creation_attempts-1 else 'No more retries remaining.'}"
                )
                if attempt == max_creation_attempts - 1:
                    logger.error_log(
                        f"Final attempt {attempt + 1} failed. Raising exception."
                    )
                    raise Exception(
                        "All attempts to create dataset returned None")
                continue

            # This won't be reached with dataset = None
            if verify_dataset_exists(bq_client, dataset.dataset_id):
                logger.info_log(
                    f"Successfully created and verified dataset on attempt {attempt + 1}"
                )
                return dataset.dataset_id, dataset.full_dataset_id

            logger.warning_log(
                f"Dataset creation verification failed on attempt {attempt + 1}/{max_creation_attempts}. "
                "Retrying creation..."
            )

        except AttributeError as e:
            logger.error_log(
                f"Attempt {attempt + 1}: AttributeError occurred: {str(e)}"
            )
            if attempt == max_creation_attempts - 1:
                raise Exception(
                    f"Dataset creation failed after {max_creation_attempts} attempts: {str(e)}"
                )

        except Exception as e:
            logger.error_log(
                f"Attempt {attempt + 1}: General exception occurred: {str(e)}"
            )
            if attempt == max_creation_attempts - 1:
                raise

        logger.info_log(f"End of attempt {attempt + 1}")
        sleep(1)

    final_msg = "Failed to create and verify dataset after all attempts"
    logger.error_log(final_msg)
    raise Exception(final_msg)

def verify_dataset_exists(bq_client: BQ, dataset_id: str):
    """Verifies if a dataset exists with retry logic"""
    max_retries = const.MAX_DATASET_VERIFICATION_RETRIES
    delay = const.INITIAL_DELAY_CHECKING_DATASET_VERIFICATION

    for attempt in range(max_retries):
        try:
            all_datasets = bq_client.list_datasets()
            existing_dataset_ids = [
                dataset.dataset_id for dataset in all_datasets]
            if dataset_id in existing_dataset_ids:
                return True

            logger.warning_log(
                f"Dataset verification attempt {attempt + 1}/{max_retries} failed. "
                f"Dataset {dataset_id} not found. Retrying in {delay} seconds."
            )
            sleep(delay)
            delay *= 2

        except Exception as e:
            logger.error_log(f"Error verifying dataset: {str(e)}")
            sleep(delay)
            delay *= 2

    return False


def cleanup_datasets(bq_client: BQ, dataset_ids: List[str]):
    """Removes datasets in case of failure"""
    for dataset_id in dataset_ids:
        try:
            bq_client.delete_dataset(dataset_id, delete_contents=True)
            logger.info_log(f"Cleaned up dataset: {dataset_id}")
        except Exception as e:
            logger.error_log(
                f"Failed to cleanup dataset {dataset_id}: {str(e)}")


def create_and_verify_pulse_datasets(org_id: str):
    """Creates and verifies BigQuery datasets"""
    db_connection = DB()
    db_connection.connect()
    bq_pulse = None
    created_dataset_ids = []
    etl_db = BillingETLDB(db_connection)

    try:
        bq_pulse = BQ(const.PROJECT_ID, const.REGION)
        bq_pulse.set_client(
            sa_utils.credentials_from_secret(
                "/secret/BILLING_ALERTS_SOURCE_SA_KEY"
            )
        )

        # Construct dataset names using org_id prefix
        export_dataset_name = f"org_{org_id}_standard_export"

        # Create Standerd Usage Cost Dataset
        export_dataset_id, export_dataset_full_id = (
            create_single_dataset_with_verification(
                bq_pulse, const.PROJECT_ID, export_dataset_name
            )
        )
        created_dataset_ids.append(export_dataset_id)
        logger.info_log(
            f"Value for 'export_dataset.full_dataset_id': {export_dataset_full_id}"
        )

        # Create table inside the export dataset
        export_table_name = f"org_{org_id}_standard_export_table"
        create_pulse_table(bq_pulse, export_dataset_id, export_table_name)
        logger.info_log(
            f"Value for 'create_pulse_table': {export_table_name}"
        )

        client_config, err = etl_db.get_client_billing_alert_config(
            org_id)
        if err is not None:
            logger.error_log(
                f"Error fetching client config for org_id {org_id}: {err}")
            return

        logger.info_log(f"client_config content: {client_config}")
        # Extract client-specific values
        client_project_id = client_config.get("projectid")
        # Update database atomically
        try:
            _, update_error = etl_db.update_table_and_dataset_values_in_db(
                org_id=int(org_id),
                project_id=client_project_id,
                updated_values={
                    "pulsebillingdataset": export_dataset_id,
                    "pulsetableid": export_table_name
                },
            )
            if update_error is not None:
                logger.error_log(
                    f"Failed to update billing_alerts_setting for org_id={org_id}, project_id={client_project_id}. Error: {update_error}"
                )

            _, update_step_error = etl_db.update_step_completed_in_user_stepper_form_step_status(
                stepid=3, org_id=org_id, step_completed=True
            )
            if update_step_error is not None:
                logger.error_log(
                    f"Failed to update step_completed status for org_id={org_id}, stepid=3, Error: {update_step_error}"
                )
        except Exception as e:
            cleanup_datasets(bq_pulse, created_dataset_ids)
            return return_type.json(
                {
                    "status_code": 500,
                    "message": str(e),
                    "datasets_created": False,
                },
                status_code=500,
            )

        return return_type.json(
            {
                "status_code": 200,
                "datasets_created": True,
                "message": "pulse datasets successfullly created in bigquery local environment",
            },
            status_code=200,
        )

    except Forbidden as e:
        logger.error_log(
            f"A 'Forbidden' type exception occurred for org-id = {org_id}. "
            f" Error: {e}, Status code: {e.code}, Error reason: {e.message}"
        )
        if bq_pulse and created_dataset_ids:
            cleanup_datasets(bq_pulse, created_dataset_ids)
        return return_type.json(
            {
                "message": "failed to create datasets (one or more permissions denied)",
                "datasets_created": False,
                "status_code": e.code,
            },
            status_code=e.code,
        )

    except Exception as e:
        logger.error_log(
            f"Failed to create datasets for org_id = {org_id}: {str(e)}")
        if bq_pulse and created_dataset_ids:
            cleanup_datasets(bq_pulse, created_dataset_ids)
        return return_type.json(
            {
                "status_code": 500,
                "message": "Failed to create datasets, contact Admin",
                "datasets_created": False,
            },
            status_code=500,
        )


def create_pulse_table(bq_client: BQ, dataset_id: str, table_name: str):
    """Creates a table inside a specified dataset if it doesn't already exist"""

    table_id = get_fully_qualified_table_id(
        const.PROJECT_ID, dataset_id, table_name)

    try:
        # Check if table already exists
        existing_table = bq_client.client.get_table(table_id)
        logger.info_log(f"Table already exists: {existing_table}")
        return existing_table
    except NotFound:
        logger.info_log(
            f"Table {table_id} not found. Proceeding to create it.")
    except Exception as e:
        logger.error_log(
            f"Error checking table existence for {table_id}: {str(e)}")
        raise

    # Define table schema
    schema = [
        bigquery.SchemaField("billing_account_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("service", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("sku", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("usage_start_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("usage_end_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("project", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("labels", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("ancestry_numbers",
                                 "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ancestors", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField(
                    "resource_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "display_name", "STRING", mode="NULLABLE"),
            ]),
        ]),
        bigquery.SchemaField("labels", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("system_labels", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("location", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zone", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("tags", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("inherited", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("namespace", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("transaction_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("export_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency_conversion_rate",
                             "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("usage", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount_in_pricing_units",
                                 "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pricing_unit", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("credits", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("invoice", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("month", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("publisher_type", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("cost_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("adjustment_info", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("mode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("cost_at_list", "FLOAT", mode="NULLABLE"),
    ]

    try:
        new_table = bigquery.Table(table_id, schema=schema)
        created_table = bq_client.client.create_table(new_table)
        logger.info_log(f"Table created: {created_table.full_table_id}")
        return created_table.full_table_id
    except Exception as e:
        logger.error_log(f"Failed to create table {table_id}: {str(e)}")
        raise


def get_fully_qualified_table_id(project_id: str, dataset_id: str, table_name: str) -> str:
    # Extract dataset name from dataset_id (handles fully qualified or plain)
    dataset_name = dataset_id.split('.')[-1]
    # Return fully qualified table ID: project.dataset.table
    return f"{project_id}.{dataset_name}.{table_name}"
