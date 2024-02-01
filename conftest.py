import pytest
from psycopg2 import pool
from unittest.mock import Mock


@pytest.fixture
def mock_os_makedirs(mocker):
    return mocker.patch("os.makedirs")


@pytest.fixture
def mock_os_rename(mocker):
    return mocker.patch("os.rename")


@pytest.fixture
def mock_gzip_open(mocker):
    return mocker.patch("gzip.open", mocker.mock_open(read_data='{"key": "value"}\n'))


@pytest.fixture
def mock_open(mocker):
    return mocker.patch("builtins.open", mocker.mock_open(read_data='{"key": "value"}'))


@pytest.fixture
def mock_connection_pool(mocker):
    mocker.patch.object(pool, "SimpleConnectionPool", autospec=True)
    mocker.patch("psycopg2.connect")  #

    yield

    # Cleanup
    mocker.stopall()


@pytest.fixture
def mocker_open(mocker):
    return mocker.patch("builtins.open", mocker.mock_open(read_data='{"key": "value"}'))


@pytest.fixture
def mock_connection(mocker):
    return mocker.patch(
        "products_etl.connect_to_postgres"
    ).return_value.__enter__.return_value


@pytest.fixture
def mock_products_data():
    return [
        {
            "sku": 123,
            "name": "Product A",
            "price": 10.0,
            "category": "Electronics",
            "popularity": 5,
        },
        {
            "sku": 456,
            "name": "Product B",
            "price": 20.0,
            "category": "Clothing",
            "popularity": 8,
        },
    ]


@pytest.fixture
def mock_postgres_connection():
    return Mock()


@pytest.fixture
def mock_erasure_request_data():
    return [
        {"customer-id": "123", "email": "test@example.com"},
        {"customer-id": "456", "email": "another@example.com"},
    ]


@pytest.fixture
def mock_date():
    return "2024-01-01"


@pytest.fixture
def mock_hour():
    return "12"
