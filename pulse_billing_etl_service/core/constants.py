import os


def getenv(env_var, fallback=""):
    return os.getenv(env_var, fallback)


REGION = getenv("REGION")

DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_NAME")
DB_HOST = getenv("DB_HOST")

PROJECT_ID = getenv("PROJECT_ID")

MAX_DATASET_CREATION_ATTEMPTS = 3
INITIAL_DELAY_CHECKING_DATASET_VERIFICATION = 0.5
MAX_DATASET_VERIFICATION_RETRIES = 3
