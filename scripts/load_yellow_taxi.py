## This script downloads NYC yellow taxi trip data for specified months and uploads it to a Snowflake database.
# It checks if the data for each month already exists locally, downloads it if not, and uploads it to a Snowflake staging area.
# The data is then copied into a specified table in the Snowflake database.
# The script uses environment variables for Snowflake connection details and logs progress using the logging module.

import os
import requests
import snowflake.connector
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

months = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05"]

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_RAW_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

DATA_DIR = "data/yellow_taxi"
os.makedirs(DATA_DIR, exist_ok=True)


def download_parquet(month: str) -> str:
    """
    Downloads NYC yellow taxi trip data parquet file for a given month if it does not already exist locally.

    Args:
        month (str): The month in 'YYYY-MM' format for which to download the data.

    Returns:
        str: The local file path to the downloaded (or existing) parquet file.

    Raises:
        requests.HTTPError: If the HTTP request for the parquet file fails.
    """
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    local_path = os.path.join(DATA_DIR, f"yellow_tripdata_{month}.parquet")
    if os.path.exists(local_path):
        logger.info(f"File {local_path} already exists. Skipping download.")
        return local_path
    logger.info(f"Downloading {month} to {local_path}")
    r = requests.get(url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)
    logger.info(f"Saved {month} to {local_path}")
    return local_path


def upload_to_snowflake(path: str) -> None:
    """
    Uploads a local file to a Snowflake staging area and copies its contents into the RAW.YELLOW_TAXI_TRIPS table.

    Args:
        path (str): The local file path to upload.

    Raises:
        Any exceptions raised by the Snowflake connector during connection or execution.

    Side Effects:
        - Uploads the specified file to the Snowflake staging area (@RAW.%YELLOW_TAXI_TRIPS).
        - Executes a COPY INTO command to load data from the staged file into the RAW.YELLOW_TAXI_TRIPS table.
        - Logs progress and status messages using the logger.
        - Closes the Snowflake cursor and connection after operation.
    """
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    cursor = conn.cursor()
    try:
        put_cmd = f"PUT file://{os.path.abspath(path)} @RAW.%YELLOW_TAXI_TRIPS AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        logger.info(f"Uploading {path} to Snowflake staging")
        cursor.execute(put_cmd)
        logger.info(f"Uploaded {path} to Snowflake staging")
        copy_cmd = "COPY INTO RAW.YELLOW_TAXI_TRIPS FROM @RAW.%YELLOW_TAXI_TRIPS FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE"
        logger.info("Copying data into raw.yellow_taxi_trips")
        cursor.execute(copy_cmd)
        logger.info("Data copied into raw.yellow_taxi_trips")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    for month in months:
        parquet_file = download_parquet(month)
        upload_to_snowflake(parquet_file)
    logger.info("All data loaded into Snowflake.")
