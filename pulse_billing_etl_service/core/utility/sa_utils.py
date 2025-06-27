from google.oauth2 import service_account


def credentials_from_secret(secret_key):
    return service_account.Credentials.from_service_account_file(secret_key)
