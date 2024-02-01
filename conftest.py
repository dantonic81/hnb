import pytest
from psycopg2 import pool


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
    # Mock psycopg2 pool.SimpleConnectionPool
    mocker.patch.object(pool, 'SimpleConnectionPool', autospec=True)
    mocker.patch("psycopg2.connect")  # Mock connect method

    yield

    # Cleanup
    mocker.stopall()


@pytest.fixture
def mocker_open(mocker):
    return mocker.patch("builtins.open", mocker.mock_open(read_data='{"key": "value"}'))
