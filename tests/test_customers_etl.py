from psycopg2 import OperationalError
from customers_etl import main
import customers_etl


def test_main(mocker):
    mock_connect_to_postgres = mocker.patch("customers_etl.connect_to_postgres")
    mocker.patch("customers_etl.process_all_data")

    main()

    assert mock_connect_to_postgres.call_count == 1
    assert customers_etl.process_all_data.call_count == 1


def test_main_with_connection_error(mocker):
    mocker.patch("customers_etl.connect_to_postgres", side_effect=OperationalError)
    mocker.patch("customers_etl.process_all_data")

    main()

    assert customers_etl.process_all_data.call_count == 0
