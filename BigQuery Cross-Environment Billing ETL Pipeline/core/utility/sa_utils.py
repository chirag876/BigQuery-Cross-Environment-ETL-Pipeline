from google.oauth2 import service_account
from google.auth import impersonated_credentials


def credentials_from_secret(secret_key):
    return service_account.Credentials.from_service_account_file(secret_key)


def get_impersonated_credentials(target_service_account_email: str):
    """Returns impersonated credentials for accessing client BigQuery environment."""
    source_credentials = service_account.Credentials.from_service_account_file(
        "/secret/BILLING_ALERTS_SOURCE_SA_KEY"
    )

    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    return impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_service_account_email,
        target_scopes=target_scopes,
        lifetime=3600,
    )