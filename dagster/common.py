from contextlib import contextmanager
import os
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime
import logging
import gzip
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Create connection pool
def create_connection_pool():
    return SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


# Context manager for acquiring and releasing a connection
@contextmanager
def get_postgres_connection():
    connection_pool = create_connection_pool()
    connection = connection_pool.getconn()

    try:
        yield connection
    finally:
        connection_pool.putconn(connection)


# Connect to PostgreSQL using the context manager
def connect_to_postgres():
    with get_postgres_connection() as connection:
        return connection


def cleanup_empty_directories(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logger.debug(f"Empty directory deleted: {dir_path}")


def archive_and_delete(file_path, dataset_type, date, hour, archive_path):
    archive_file = dataset_type
    archive_file_path = os.path.join(archive_path, date, hour, archive_file)

    # Create the archive directory if it doesn't exist
    os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)

    # Archive the file
    os.rename(file_path, archive_file_path)
    logger.debug(f"File archived: {archive_file_path}")


def extract_actual_date(date_str):
    return datetime.strptime(date_str.replace("date=", ""), "%Y-%m-%d").date()


# Extract the actual hour from the "hour=00" format
def extract_actual_hour(hour_str):
    return int(hour_str.replace("hour=", ""))


def log_processing_statistics(connection, date, hour, dataset_type, record_count, processing_time):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO data.processing_statistics (record_date, record_hour, dataset_type, record_count, processing_time) 
            VALUES (%s, %s, %s, %s, %s);
        """, (actual_date, actual_hour, dataset_type, record_count, processing_time))
    connection.commit()


def extract_data(file_path):
    if not file_path:
        return []

    _, file_extension = os.path.splitext(file_path)

    if file_extension == '.gz':
        print(f"FILE_PATH = {file_path}")
        # Extract raw_data from a gzipped JSON file
        with gzip.open(file_path, "rt") as file:
            data = [json.loads(line) for line in file]
    elif file_extension == '.json':
        # Extract raw_data from a plain JSON file
        with open(file_path, "r", encoding="utf-8") as file:
            data = [json.load(file)]
    else:
        print(f"Unsupported file format: {file_extension}")
        return []

    return data
