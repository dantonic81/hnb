from unittest.mock import patch
from transactions_etl import (
    is_existing_product,
    are_valid_product_skus,
    is_valid_total_cost,
    process_hourly_data
)


def test_is_existing_product(mock_connection):
    # Mocking the cursor and its execute method
    mock_cursor = mock_connection.cursor.return_value
    mock_context_manager = mock_cursor.__enter__.return_value
    mock_context_manager.fetchone.return_value = (1,)

    # Testing the function
    result = is_existing_product(mock_connection, "123")

    assert result is True

    expected_query = """
            SELECT COUNT(*) FROM data.products
            WHERE sku = %s;
    """.strip()

    actual_calls = mock_context_manager.execute.mock_calls
    assert len(actual_calls) == 1
    actual_query = actual_calls[0][1][0].strip()

    print(f"Expected query: {repr(expected_query)}")
    print(f"Actual query: {repr(actual_query)}")
    assert actual_query == expected_query


def test_are_valid_product_skus(mock_connection):
    # Mocking is_existing_product function
    with patch("transactions_etl.is_existing_product", return_value=True):
        products = [{"sku": "SKU1"}, {"sku": "SKU2"}]
        result = are_valid_product_skus(mock_connection, products)
        assert result is True


def test_is_valid_total_cost():
    products = [{"price": 10, "quanitity": 2}, {"price": 5, "quanitity": 3}]
    result = is_valid_total_cost(products, "35")
    assert result is True


def test_process_hourly_data(mock_connection):
    with patch("transactions_etl.extract_data", return_value=[{"transaction_id": "123"}]):
        with patch("transactions_etl.transform_and_validate_transactions", return_value=[{"transaction_id": "123"}]):
            with patch("transactions_etl.load_data"):
                with patch("transactions_etl.log_processed_transactions"):
                    with patch("transactions_etl.archive_and_delete"):
                        process_hourly_data(mock_connection, "2022-01-01", "01", ["transactions.json"])
