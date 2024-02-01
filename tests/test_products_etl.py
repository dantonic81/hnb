from unittest.mock import patch
from products_etl import (process_hourly_data, process_all_data)


def test_process_hourly_data(mock_connection, mock_products_data, mocker):
    date = "2022-01-01"
    hour = "12"
    available_datasets = ["products.json.gz"]

    # Mock os.makedirs and os.rename using pytest mocker
    mocker.patch("os.makedirs")
    mocker.patch("os.rename")

    # Mock the open function to avoid FileNotFoundError
    mocker.patch("builtins.open", mocker.mock_open())

    with mocker.patch("products_etl.extract_data", return_value=mock_products_data):
        process_hourly_data(mock_connection, date, hour, available_datasets)

    # Assert or perform other checks as needed


def test_process_all_data(mock_connection):
    with patch("products_etl.os.listdir", return_value=["2022-01-01"]):
        with patch("products_etl.os.path.join", side_effect=lambda *args: "/".join(args)):
            with patch("products_etl.os.listdir", return_value=["12"]):
                with patch("products_etl.os.listdir", return_value=["products.json.gz"]):
                    process_all_data(mock_connection)

    # Add assertions for the expected behavior during all data processing


# You can add more test cases for edge cases, exceptions, etc.
