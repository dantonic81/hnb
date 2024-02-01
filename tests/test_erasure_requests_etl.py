import os
from datetime import datetime
from unittest.mock import Mock, patch
from erasure_requests_etl import (
    get_date_and_hour_to_anonymize,
    anonymize_and_update_data,
    archive_updated_file,
    process_erasure_requests,
)


def test_get_date_and_hour_to_anonymize(mock_connection):
    with patch(
        "erasure_requests_etl.connect_to_postgres", return_value=mock_connection
    ):
        mock_connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (
            datetime(2024, 1, 1),
            12,
        )

        result = get_date_and_hour_to_anonymize(mock_connection, "123")
        assert result == (datetime(2024, 1, 1), 12)


def test_anonymize_and_update_data(tmp_path, mock_erasure_request_data):
    file_path = tmp_path / "test.json"
    file_path.write_text(
        '{"id": "123", "email": "test@example.com"}\n{"id": "456", "email": "another@example.com"}'
    )

    anonymize_and_update_data(str(file_path), "123", mock_erasure_request_data[0])

    data = file_path.read_text()
    assert "test@example.com" not in data
    assert "another@example.com" in data


def test_archive_updated_file(tmp_path, mock_date, mock_hour, monkeypatch):
    file_path = tmp_path / "test.json"
    file_path.touch()

    # Mock os.makedirs to avoid actual filesystem operations
    monkeypatch.setattr(os, "makedirs", Mock())

    # Mock os.rename to avoid actual filesystem operations
    monkeypatch.setattr(os, "rename", Mock())

    archive_updated_file(str(file_path), mock_date, mock_hour)

    # Add assertions based on your specific logic
    assert os.makedirs.called_once_with(
        "/opt/dagster/app/archived_data/2024-01-01/12", exist_ok=True
    )
    assert os.rename.called_once_with(
        str(file_path), "/opt/dagster/app/archived_data/2024-01-01/12/test.json"
    )


def test_process_erasure_requests(mock_postgres_connection, mock_erasure_request_data):
    with patch(
        "erasure_requests_etl.get_date_and_hour_to_anonymize",
        return_value=(datetime(2024, 1, 1), 12),
    ):
        with patch(
            "erasure_requests_etl.locate_processed_data_file",
            return_value="/path/to/processed_data.json",
        ):
            with patch("erasure_requests_etl.anonymize_and_update_data"):
                with patch("erasure_requests_etl.archive_updated_file"):
                    process_erasure_requests(
                        mock_postgres_connection, mock_erasure_request_data
                    )
                    # Add assertions based on your specific logic
