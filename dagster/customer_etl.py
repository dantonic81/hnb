import gzip
import hashlib
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = '/opt/dagster/app/raw_data'
PROCESSED_DATA_PATH = '/opt/dagster/app/processed_data'
ARCHIVED_DATA_PATH = '/opt/dagster/app/archived_data'


connection_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

# Helper functions
# TODO schema validation json
# TODO prevent duplicate loading
# TODO handle missing fields and duplicate skus in transformations
# TODO add orchestration / workflows for different arrivals
# TODO add tests
# TODO connect everything to minio
# TODO log records in database for erasure requests and have single source of truth
# TODO prevent duplicates from loading
# todo forget minio and loading directly in container, use volume mounts instead


def get_connection():
    return connection_pool.getconn()


def release_connection(conn):
    connection_pool.putconn(conn)


def connect_to_postgres():
    return get_connection()


# Create the processed_data_log table
def create_processed_data_log_table(connection, schema_name):
    with connection.cursor() as cursor:
        # Create the schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        # Set the search path to include the specified schema
        cursor.execute(f"SET search_path TO {schema_name};")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_data_log (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                hour INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    connection.commit()


def extract_data(file_path):
    if not file_path:
        return []

    # Extract raw_data from a gzipped JSON file
    with gzip.open(file_path, "rt") as file:
        data = [json.loads(line) for line in file]
    return data


def transform_and_validate_customers(customers_data):
    # Transform and validate customer raw_data
    # Ensure required fields are populated, id is unique, and other constraints
    unique_ids = set()

    for customer in customers_data:
        if 'id' not in customer or 'first_name' not in customer or 'last_name' not in customer or 'email' not in customer:
            # Log or handle missing required fields
            continue

        if customer['id'] in unique_ids:
            # Log or handle duplicate ids
            continue

        # Additional validation logic (e.g., email format, date_of_birth format)

        # Update last_change timestamp
        customer['last_change'] = datetime.utcnow().isoformat()

        unique_ids.add(customer['id'])

    return customers_data


def extract_actual_date(date_str):
    return datetime.strptime(date_str.replace("date=", ""), "%Y-%m-%d").date()


# Extract the actual hour from the "hour=00" format
def extract_actual_hour(hour_str):
    return int(hour_str.replace("hour=", ""))


def log_processed_customers(connection, date, hour, customer_ids):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for customer_id in customer_ids:
            cursor.execute("""
                INSERT INTO processed_data_log (date, hour, customer_id) 
                VALUES (%s, %s, %s);
            """, (actual_date, actual_hour, customer_id))
    connection.commit()


def load_data(data, dataset_type, date, hour):
    logger.info(f"Loading data {data} for {dataset_type} {date} {hour}")
    # Create the corresponding subdirectories in processed_data
    output_dir = os.path.join(PROCESSED_DATA_PATH, date, hour)
    os.makedirs(output_dir, exist_ok=True)

    # Determine the appropriate file extension based on dataset_type
    if dataset_type.endswith(".json.gz"):
        file_extension = ".json.gz"
    elif dataset_type.endswith(".json"):
        file_extension = ".json"
    else:
        raise ValueError(f"Unsupported file extension in dataset_type: {dataset_type}")

    # Remove the existing extension if present
    dataset_type_without_extension, _ = dataset_type.split(".", 1)

    # Load processed data to the new location only if the dataset is not empty
    if data:
        # Use gzip compression if the file extension is .json.gz
        open_func = gzip.open if file_extension == ".json.gz" else open
        # Construct the output path
        output_path = os.path.join(output_dir, f"{dataset_type_without_extension}{file_extension}")

        with open_func(output_path, "wt") as file:
            for record in data:
                json.dump(record, file)
                file.write("\n")
    else:
        logger.debug(f"Skipping loading for empty dataset: {dataset_type}")


# def archive_and_delete(file_path, dataset_type, date, hour, archive_path):
#     archive_file = dataset_type
#     archive_file_path = os.path.join(archive_path, date, hour, archive_file)
#
#     # Create the archive directory if it doesn't exist
#     os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)
#
#     # Archive the file
#     os.rename(file_path, archive_file_path)
#     logger.debug(f"File archived: {archive_file_path}")


def process_hourly_data(connection, date, hour, available_datasets):
    print(date, hour, len(available_datasets), available_datasets)
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    print("Dataset Paths:", dataset_paths)

    # Extract raw_data
    customers_data = extract_data(dataset_paths.get("customers.json.gz", ""))
    print("Number of customers:", len(customers_data))

    # Transform and validate raw_data
    transformed_customers = transform_and_validate_customers(customers_data)

    # Load processed raw_data
    load_data(transformed_customers, "customers.json.gz", date, hour)

    # Log processed customers
    customer_ids = [customer["id"] for customer in transformed_customers]
    log_processed_customers(connection, date, hour, customer_ids)

    # Archive and delete the original files
    # for dataset_type, dataset_path in dataset_paths.items():
    #     # print("Processing dataset:", dataset_type, "Path:", dataset_path)
    #     archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data():
    schema_name = "data"
    connection = connect_to_postgres()
    create_processed_data_log_table(connection, schema_name)
    # Get a sorted list of date folders
    try:
        date_folders = os.listdir(RAW_DATA_PATH)
        date_folders.sort()
        print("Date Folders:", date_folders)
        # Process all available raw_data
        for date_folder in date_folders:
            date_path = os.path.join(RAW_DATA_PATH, date_folder)

            # Get a sorted list of hour folders
            hour_folders = os.listdir(date_path)
            hour_folders.sort()
            print(f"Hour Folders for {date_folder}:", hour_folders)

            for hour_folder in hour_folders:
                hour_path = os.path.join(date_path, hour_folder)

                available_datasets = [filename for filename in os.listdir(hour_path) if filename.startswith("customers")
                                      and filename.endswith((".json", ".json.gz"))]

                if available_datasets:
                    process_hourly_data(connection, date_folder, hour_folder, available_datasets)
                else:
                    logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

        #
        # # Clean up empty directories in raw_data after processing
        # cleanup_empty_directories(RAW_DATA_PATH)
    finally:
        release_connection(connection)


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
