import random
import time
from datetime import date, datetime
from typing import Optional

import core.constants as const
from core.database.billing_etl_db import BillingETLDB
from core.database.database_class import DB
from core.models.billing_etl_model import BillingETLMessage
from core.utility import logger
from core.utility.dataset_utils import create_and_verify_pulse_datasets
from google.api_core.exceptions import NotFound
from google.auth import impersonated_credentials
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, Conflict


def get_impersonated_credentials(target_service_account_email: str):
    source_credentials = service_account.Credentials.from_service_account_file(
        "/secret/BILLING_ALERTS_SOURCE_SA_KEY")

    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_service_account_email,
        target_scopes=target_scopes,
        lifetime=3600,
    )
    return impersonated_creds


def serialize_row(row):
    return {
        key: (value.isoformat() if isinstance(
            value, (datetime, date)) else value)
        for key, value in row.items()
    }


def process_etl_job(pubsub_message: dict, context: Optional[dict] = None):
    """Handles ETL job triggered by Pub/Sub, processes the billing data,
    and logs the outcome to the status table.
    """
    # Log the received
    logger.info_log(f"Received pubsub_message: {pubsub_message}")
    try:
        db_connection = DB()
        db_connection.connect()
        if db_connection.connection is None:
            logger.error_log(
                "DB connection is None after attempting to connect.")
            return
        etl_db = BillingETLDB(db_connection)
        message: BillingETLMessage = None
        max_retries = 3
        # Parse the Pub/Sub message
        message = BillingETLMessage(**pubsub_message)

        logger.info_log(f"Parsed message: {message}")

        # Fetch client billing alert configuration using org_id
        client_config, err = etl_db.get_client_billing_alert_config(
            message.org_id)
        if err is not None:
            logger.error_log(
                f"Error fetching client config for org_id {message.org_id}: {err}")
            return

        logger.info_log(f"client_config content: {client_config}")
        # Extract client-specific values
        client_project_id = client_config.get("projectid")

        client_billing_dataset_id = client_config.get("billingdataset")

        client_table_id = client_config.get("tableid")

        destination_dataset = client_config.get("pulsebillingdataset")

        if not destination_dataset:
            logger.info_log(
                f"pulsebillingdataset is null for org_id {message.org_id}. Attempting to create the dataset...")
            response = create_and_verify_pulse_datasets(message.org_id)
            if response.status_code == 200:
                logger.info_log(
                    f"Dataset created. Re-fetching updated config...")
                client_config, err = etl_db.get_client_billing_alert_config(
                    message.org_id)
                if err is not None:
                    logger.error_log(
                        f"Error fetching client config for org_id {message.org_id}: {err}")
                    return
                destination_dataset = client_config.get("pulsebillingdataset")
                if not destination_dataset:
                    logger.error_log(
                        f"pulsebillingdataset still null after creation for org_id {message.org_id}")
                    return
            else:
                logger.error_log(
                    f"Failed to create dataset for org_id {message.org_id}")
                return

        logger.info_log(
            f"Client config: project_id={client_project_id}, dataset={client_billing_dataset_id}, table={client_table_id}")

        if not client_project_id or not client_billing_dataset_id or not client_table_id:

            logger.error_log(
                f"Missing billing information for org_id {message.org_id}")
            return

        # Construct destination table name using org_id
        pulse_project_id = const.PROJECT_ID

        pulse_table_id = f"org_{message.org_id}_standard_export_table"

        destination_table = f"{pulse_project_id}.{destination_dataset}.{pulse_table_id}"

        # Fetch start_date_time from last successful run, else use 1970
        client_project_id = client_config.get("projectid")

        client_sa_email = client_config.get(
            "customerserviceaccountid")  # or appropriate key

        impersonated_creds = get_impersonated_credentials(client_sa_email)

        bq_client = bigquery.Client(
            project=client_project_id, credentials=impersonated_creds)

        # For writing to zenta project
        destination_bq_client = bigquery.Client(project=const.PROJECT_ID)

        last_success_ts = etl_db.get_last_success_timestamp(
            message.org_id, client_project_id)

        start_date_time = last_success_ts if last_success_ts else datetime(
            1970, 1, 1)

        logger.info_log(
            f"start_date_time: {start_date_time}")

        for attempt in range(max_retries):
            try:
                logger.info_log(
                    f"Running ETL for org_id {message.org_id}, attempt {attempt + 1}")

                # Extract data from the client's BigQuery dataset
                data = extract_data_from_bq(
                    bq_client, client_project_id, client_billing_dataset_id, client_table_id,
                    start_date_time, datetime.now(),  # pass current time as end time for querying all up to now
                    message,
                    batch_size=1000
                )

                # materialize result set to list for processing
                rows = list(data)

                if not rows:
                    # No new data found, so set end_date_time = datetime.now() to avoid looping forever
                    end_date_time = datetime.now()
                    logger.info_log(
                        f"No new data found after {start_date_time}. Setting end_date_time to now.")
                else:
                    # Find max export_time in the batch
                    max_export_time = max(row['export_time'] for row in rows)
                    end_date_time = max_export_time

                logger.info_log(
                    f"Extracted {len(rows)} rows between {start_date_time} and {end_date_time}")

                etl_db.save(
                    status="IN_PROGRESS",
                    org_id=message.org_id,
                    project_id=client_project_id,
                    end_date_time=end_date_time
                )
                # Transform data (if needed)
                transformed_data = transform_data(data)

                # Ensure dataset exists before loading
                ensure_dataset_exists(
                    destination_bq_client, pulse_project_id, destination_dataset, message.org_id)

                # Load data into the destination dataset
                load_result = load_data_to_bq(destination_bq_client,
                                              transformed_data, destination_table)

                if load_result.get("status") != "SUCCESS":
                    raise Exception(f"Failed loading data: {load_result}")
                # Log SUCCESS
                etl_db.save(
                    status="SUCCESS",
                    org_id=message.org_id,
                    project_id=client_project_id,
                    end_date_time=end_date_time
                )
                logger.info_log(f"ETL succeeded for org_id {message.org_id}")
                return

            except Exception as e:
                logger.error_log(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(0, 2 ** attempt)
                    logger.info_log(
                        f"[Retry Attempt {attempt + 1}/{max_retries}] Waiting for {sleep_time:.2f} seconds before the next retry attempt due to failure.")
                    time.sleep(sleep_time)
                else:
                    # Final failure
                    etl_db.save(
                        status="FAILED",
                        org_id=message.org_id,
                        project_id=client_project_id,
                        end_date_time=end_date_time
                    )
                    logger.error_log(
                        f"All {max_retries} attempts failed for org_id {message.org_id}. Final error: {str(e)}")
                    return

    except Exception as e:
        logger.error_log(f"ETL failed before execution: {str(e)}")


def ensure_dataset_exists(bq_client: bigquery.Client, project_id: str, dataset_id: str, org_id: str):
    """Checks if the dataset exists; if not, creates it."""
    try:
        bq_client.get_dataset(f"{project_id}.{dataset_id}")
        logger.info_log(f"Dataset {project_id}.{dataset_id} already exists.")
    except NotFound:
        logger.info_log(
            f"Dataset {project_id}.{dataset_id} not found. Attempting to create it...")
        try:
            response = create_and_verify_pulse_datasets(org_id)

            if response.status_code == 200:
                logger.info_log(
                    f"Dataset creation successful for org_id: {org_id}")
            else:
                logger.error_log(
                    f"Dataset creation failed for org_id: {org_id}, status: {response.status_code}")
        except Conflict:
            logger.info_log(
                f"Dataset {project_id}.{dataset_id} already exists (Conflict). Continuing ETL.")
        except Exception as e:
            logger.error_log(
                f"Exception while creating dataset for org_id: {org_id}: {str(e)}")
            raise


def count_rows_in_bq(bq_client: Client, client_project_id, client_billing_dataset_id, client_table_id, start_date_time: datetime, end_date_time: datetime):
    """Count total rows between start and end time."""
    count_query = f"""
    SELECT COUNT(*) as total_count FROM `{client_project_id}.{client_billing_dataset_id}.{client_table_id}`
    WHERE export_time >= TIMESTAMP('{start_date_time.isoformat()}')
    AND export_time < TIMESTAMP('{end_date_time.isoformat()}')
    """
    try:
        count_job = bq_client.query(count_query)
        count_result = count_job.result()
        total_count = 0
        for row in count_result:
            total_count = row.total_count
        logger.info_log(f"Total rows to extract: {total_count}")
        return total_count
    except Exception as e:
        logger.error_log(f"Error counting rows: {str(e)}")
        raise e

def extract_data_from_bq(bq_client: Client, client_project_id, client_billing_dataset_id, client_table_id, start_date_time: datetime, end_date_time: datetime, message: BillingETLMessage, batch_size=1000):
    """Extract data in batches of `batch_size` from BigQuery within the given time bounds."""

    total_rows = count_rows_in_bq(bq_client, client_project_id, client_billing_dataset_id, client_table_id, start_date_time, end_date_time)

    all_data = []
    for offset in range(0, total_rows, batch_size):
        logger.info_log(f"Extracting batch with OFFSET {offset} and LIMIT {batch_size}")
        query = f"""
        SELECT * FROM `{client_project_id}.{client_billing_dataset_id}.{client_table_id}`
        WHERE export_time >= TIMESTAMP('{start_date_time.isoformat()}')
        AND export_time < TIMESTAMP('{end_date_time.isoformat()}')
        LIMIT {batch_size} OFFSET {offset}
        """
        try:
            query_job = bq_client.query(query)
            batch_data = query_job.result()
            batch_list = list(batch_data)
            logger.info_log(f"Extracted {len(batch_list)} rows for org_id: {message.org_id} in current batch")
            all_data.extend(batch_list)
            if len(batch_list) < batch_size:
                # Last batch received less than batch size - no more data expected
                break
        except Exception as e:
            logger.error_log(f"Error extracting batch with offset {offset}: {str(e)}")
            raise e

    logger.info_log(f"Total extracted rows: {len(all_data)}")
    return all_data


def transform_data(data):
    """Applies transformations to the extracted data (placeholder)."""
    return data


def load_data_to_bq(destination_bq_client: bigquery.Client, data, destination_table: str):
    try:
        destination_bq_client.get_table(destination_table)

        batch_size = 1000
        batch = []
        failed_batches = 0
        batch_index = 0

        for row in data:
            batch.append(serialize_row(dict(row.items())))

            if len(batch) == batch_size:
                if not upload_batch(destination_bq_client, batch, destination_table, batch_index):
                    failed_batches += 1
                batch = []  # Reset for next batch
                batch_index += 1

        # Load remaining rows if any
        if batch:
            if not upload_batch(destination_bq_client, batch, destination_table, batch_index):
                failed_batches += 1

        if failed_batches == 0:
            return {"status": "SUCCESS", "code": 200}
        elif failed_batches < batch_index + 1:
            return {"status": "PARTIAL_SUCCESS", "code": 206, "failed_batches": failed_batches}
        else:
            return {"status": "FAILED", "code": 500, "failed_batches": failed_batches}

    except Exception as e:
        logger.error_log(
            f"Failed to load data into {destination_table}: {str(e)}")
        return {"status": "FAILED", "code": 500, "error": str(e)}


def upload_batch(client: bigquery.Client, batch, table, batch_number):
    retries = 0
    while retries < 3:
        try:
            job = client.load_table_from_json(batch, table)
            job.result()
            logger.info_log(
                f"Loaded batch {batch_number + 1} with {len(batch)} rows into {table}")
            return True
        except Exception as e:
            if 'rateLimitExceeded' in str(e):
                wait = 2 ** retries
                logger.info_log(
                    f"Rate limit hit for batch {batch_number + 1}, retrying in {wait} seconds...")
                time.sleep(wait)
                retries += 1
            else:
                logger.error_log(f"Batch {batch_number + 1} failed: {e}")
                return False
    logger.error_log(f"Batch {batch_number + 1} failed after retries.")
    return False
