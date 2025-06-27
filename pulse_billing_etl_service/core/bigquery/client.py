from core.utility import logger, sa_utils
from google.cloud import bigquery


class BQ:
    client = None
    project = None
    location = None

    def __init__(self, project, location=None) -> None:
        self.project = project
        self.location = location

    def set_client(self, credentials=None):
        self.client = bigquery.Client(
            project=self.project, credentials=credentials)

    def set_impersonated_client(self, sa_key_secret_path, target_service_account):
        creds = sa_utils.get_impersonated_credentials(
            sa_key_secret_path, target_service_account
        )
        if creds is None:
            raise Exception(
                f"Either 'credentials' or 'client' value is null for project = {self.project}.",
            )

        self.set_client(creds)

    def execute(self, query):
        query_job = self.client.query(query)
        return query_job.result()

    def list_projects(self):
        return self.client.list_projects()

    def list_datasets(self):
        return self.client.list_datasets(self.project)

    def list_tables(self, dataset):
        return self.client.list_tables(dataset)

    def create_new_dataset(self, dataset_id, location=None):
        dataset = bigquery.Dataset(dataset_ref=dataset_id)
        dataset.location = location if location is not None else self.location
        dataset = self.client.create_dataset(dataset, timeout=30)
        return dataset

    def delete_dataset(self, dataset_id, delete_contents=True, not_found_ok=True):
        try:
            self.client.delete_dataset(
                dataset=dataset_id,
                delete_contents=delete_contents,
                not_found_ok=not_found_ok,
            )
            logger.info_log(f"Successfully deleted dataset: {dataset_id}")
        except Exception as e:
            logger.error_log(f"Error deleting dataset {dataset_id}: {str(e)}")
            raise

    def get_client(self):
        return self.client
