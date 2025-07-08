import os
from unittest.mock import patch, MagicMock, mock_open
from scripts.load_yellow_taxi import download_parquet, upload_to_snowflake


@patch("scripts.load_yellow_taxi.requests.get")
def test_download_parquet(mock_get: MagicMock) -> None:
    """
    Test the `download_parquet` function to ensure it correctly downloads a parquet file for a given month.

    This test:
    - Mocks the HTTP GET request to return a successful response with dummy parquet data.
    - Mocks the file system to simulate that the target file does not exist.
    - Mocks the file opening and writing process.
    - Verifies that the correct URL is requested.
    - Verifies that the file is opened in write-binary mode at the expected local path.
    - Asserts that the result contains the expected local file path.
    """
    month = "2025-01"
    expected_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    local_path = f"data/yellow_taxi/yellow_tripdata_{month}.parquet"

    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"parquet data"

    with (
        patch("scripts.load_yellow_taxi.os.path.exists", return_value=False),
        patch("builtins.open", mock_open()) as mock_file,
    ):
        result = download_parquet(month)

        mock_get.assert_called_once_with(expected_url)
        mock_file.assert_called_once_with(local_path, "wb")
        assert str(local_path) in result


@patch("scripts.load_yellow_taxi.snowflake.connector.connect")
def test_upload_to_snowflake(mock_connect: MagicMock) -> None:
    """
    Test the `upload_to_snowflake` function to ensure it interacts with the Snowflake connection and cursor as expected.

    This test verifies that:
    - A connection is established using the provided mock.
    - The correct SQL commands are executed to upload a Parquet file and copy its contents into the target table.
    - The cursor and connection are properly closed after the operation.

    Mocks:
        mock_connect: Mocked Snowflake connection object.

    Test Steps:
        1. Set up a mock cursor and assign it to the mock connection.
        2. Call `upload_to_snowflake` with a fake file path.
        3. Assert that the connection is established.
        4. Assert that the correct SQL commands are executed for file upload and data copy.
        5. Assert that the cursor and connection are closed.
    """
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor

    fake_path = "fake_path.parquet"

    upload_to_snowflake(fake_path)

    mock_connect.assert_called_once()
    mock_cursor.execute.assert_any_call(
        f"PUT file://{os.path.abspath(fake_path)} @RAW.%YELLOW_TAXI_TRIPS AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    mock_cursor.execute.assert_any_call(
        "COPY INTO RAW.YELLOW_TAXI_TRIPS FROM @RAW.%YELLOW_TAXI_TRIPS FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE"
    )
    mock_cursor.close.assert_called_once()
    mock_connect.return_value.close.assert_called_once()
