from common import (
    create_connection_pool,
    get_postgres_connection,
    connect_to_postgres,
    archive_and_delete,
    extract_actual_date,
    extract_actual_hour,
    extract_data,
    load_data,
)
import datetime
from datetime import datetime


def test_create_connection_pool(mock_connection_pool):
    assert create_connection_pool() is not None


def test_get_postgres_connection(mock_connection_pool):
    assert get_postgres_connection() is not None


def test_connect_to_postgres(mock_connection_pool):
    assert connect_to_postgres() is not None


def test_archive_and_delete(mock_os_rename, mocker):
    mocker.patch("common.os.makedirs")
    archive_and_delete("file.json", "type", "2022-01-01", "00", "/archive")
    mock_os_rename.assert_called_once_with("file.json", "/archive/2022-01-01/00/type")


def test_extract_actual_date():
    result = extract_actual_date("date=2022-01-01")
    # Create a datetime.date object for the expected value
    expected_date = datetime(2022, 1, 1).date()

    assert result == expected_date


def test_extract_actual_hour():
    result = extract_actual_hour("hour=05")
    assert result == 5


def test_extract_data(mock_gzip_open, mock_open):
    result_gzip = extract_data("file.json.gz")
    result_json = extract_data("file.json")
    assert result_gzip == [{"key": "value"}]
    assert result_json == [{"key": "value"}]


def test_load_data_empty_dataset(mocker, mock_os_makedirs, mocker_open):
    mocker.patch("common.os.makedirs")
    mocker.patch("common.os.path.join")
    mocker.patch("common.logger.debug")

    load_data([], "type.json", "2022-01-01", "00", "/processed_data")
    mocker_open.assert_not_called()
