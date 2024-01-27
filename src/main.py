import logging
import os
import json
import gzip
import hashlib
from datetime import datetime
from dotenv import load_dotenv

import psycopg2
from psycopg2.pool import SimpleConnectionPool

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'raw_data')
PROCESSED_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'processed_data')
ARCHIVED_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'archived_data')

connection_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)


# Acquire a connection from the pool
def get_connection():
    return connection_pool.getconn()


# Release the connection back to the pool
def release_connection(conn):
    connection_pool.putconn(conn)


# Helper functions
# TODO schema validation json
# TODO prevent duplicate loading
# TODO handle missing fields and duplicate skus in transformations
# TODO add orchestration / workflows for different arrivals
# TODO add tests
# TODO connect everything to minio
# TODO log records in database for erasure requests and have single source of truth


# =========================== DATABASE FUNCTIONS
# Connect to PostgreSQL
def connect_to_postgres():
    return get_connection()


# Create the processed_data_log table
def create_processed_data_log_table(connection):
    with connection.cursor() as cursor:
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


# Extract the actual date from the "date=20xx-xx-xx" format
def extract_actual_date(date_str):
    return datetime.strptime(date_str.replace("date=", ""), "%Y-%m-%d").date()


# Extract the actual hour from the "hour=00" format
def extract_actual_hour(hour_str):
    return int(hour_str.replace("hour=", ""))


# Insert a record into processed_data_log for each processed customer
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


# Query the processed_data_log table to get date and hour for a given customer_id
# def query_processed_data_log(connection, customer_id):
#     with connection.cursor() as cursor:
#         cursor.execute("""
#             SELECT date, hour FROM processed_data_log
#             WHERE customer_id = %s
#         """, (customer_id,))
#         result = cursor.fetchone()
#         if result:
#             return result
#     return None


# Locate the processed data file based on date and hour
# def locate_processed_data_file(date, hour):
#     processed_data_path = os.path.join(PROCESSED_DATA_PATH, str(date), str(hour))
#     for filename in os.listdir(processed_data_path):
#         if filename.endswith(".json"):
#             return os.path.join(processed_data_path, filename)
#     return None


# Anonymize and update the data in the processed data file
# def anonymize_and_update_data(file_path, customer_id, erasure_request):
#     email_to_anonymize = erasure_request.get("email")
#     anonymized_email = hashlib.sha256(email_to_anonymize.encode()).hexdigest()
#
#     with open(file_path, "r") as file:
#         data = [json.loads(line) for line in file]
#
#     for record in data:
#         if record.get("id") == customer_id:
#             # Anonymize the email in the record
#             record["email"] = anonymized_email
#
#     with open(file_path, "w") as file:
#         for record in data:
#             json.dump(record, file)
#             file.write("\n")


# Archive the updated file
# def archive_updated_file(file_path, date, hour):
#     archive_path = os.path.join(ARCHIVED_DATA_PATH, str(date), str(hour))
#     os.makedirs(archive_path, exist_ok=True)
#
#     archive_file = os.path.basename(file_path)
#     archive_file_path = os.path.join(archive_path, archive_file)
#
#     os.rename(file_path, archive_file_path)
#     logger.info(f"File archived: {archive_file_path}")


# def process_erasure_requests(connection, erasure_requests):
#     for erasure_request in erasure_requests:
#         customer_id = erasure_request.get("customer-id")
#         if customer_id:
#             # Step 1: Query the processed_data_log table to get date and hour
#             result = query_processed_data_log(connection, customer_id)
#             if result:
#                 date, hour = result
#
#                 # Step 2: Locate the processed data file
#                 file_path = locate_processed_data_file(date, hour)
#
#                 if file_path:
#                     # Step 3: Anonymize and update the data
#                     anonymize_and_update_data(file_path, customer_id, erasure_request)
#
#                     # Step 4: Archive the updated file
#                     archive_updated_file(file_path, date, hour)
# # ================================ END OF DATABASE FUNCTIONS


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


def transform_and_validate_products(products_data):
    # Transform and validate product raw_data
    # Ensure all fields are populated, sku is unique, and other constraints
    unique_skus = set()

    for product in products_data:
        if 'sku' not in product or 'name' not in product or 'price' not in product or 'popularity' not in product:
            # Log or handle missing required fields
            continue

        if product['sku'] in unique_skus:
            # Log or handle duplicate skus
            continue

        # Additional validation logic (e.g., price format, popularity range)

        unique_skus.add(product['sku'])

    return products_data


def transform_and_validate_transactions(transactions_data):
    # Transform and validate transactions raw_data
    # Ensure unique transaction_id, valid customer_id, valid product skus, and total_cost matches total
    unique_transaction_ids = set()

    for transaction in transactions_data:
        if 'transaction_id' not in transaction or 'customer_id' not in transaction or 'purchases' not in transaction:
            # Log or handle missing required fields
            continue

        if transaction['transaction_id'] in unique_transaction_ids:
            # Log or handle duplicate transaction_ids
            continue

        # Additional validation logic (e.g., valid customer_id, valid product skus, total_cost calculation)

        unique_transaction_ids.add(transaction['transaction_id'])

    return transactions_data


def anonymize_customer_data(customer_data, erasure_requests):
    # Anonymize customer raw_data based on erasure requests
    # Use hashing or another anonymization technique

    for erasure_request in erasure_requests:
        customer_id = erasure_request.get("customer-id")
        email = erasure_request.get("email")

        for customer in customer_data:
            if customer_id and customer["id"] == customer_id:
                # Anonymize personally identifiable information
                customer["email"] = hashlib.sha256(email.encode()).hexdigest()
            elif email and customer["email"] == email:
                # Anonymize personally identifiable information
                customer["email"] = hashlib.sha256(email.encode()).hexdigest()

    return customer_data


def load_data(data, dataset_type, date, hour):
    logger.debug(f"Loading data for {dataset_type}")
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


def archive_and_delete(file_path, dataset_type, date, hour, archive_path):
    archive_file = dataset_type
    archive_file_path = os.path.join(archive_path, date, hour, archive_file)

    # Create the archive directory if it doesn't exist
    os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)

    # Archive the file
    os.rename(file_path, archive_file_path)
    logger.debug(f"File archived: {archive_file_path}")


def process_hourly_data(connection, date, hour, available_datasets):
    print(date, hour, len(available_datasets), available_datasets)
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    print("Dataset Paths:", dataset_paths)

    # Extract raw_data
    customers_data = extract_data(dataset_paths.get("customers.json.gz", ""))
    products_data = extract_data(dataset_paths.get("products.json.gz", ""))
    transactions_data = extract_data(dataset_paths.get("transactions.json.gz", ""))
    erasure_requests_data = extract_data(dataset_paths.get("erasure-requests.json.gz", ""))

    # Transform and validate raw_data
    transformed_customers = transform_and_validate_customers(customers_data)
    transformed_products = transform_and_validate_products(products_data)
    transformed_transactions = transform_and_validate_transactions(transactions_data)

    # Anonymize customer raw_data
    anonymized_customers = anonymize_customer_data(transformed_customers, erasure_requests_data)

    # Load processed raw_data
    load_data(anonymized_customers, "customers.json.gz", date, hour)
    load_data(transformed_products, "products.json.gz", date, hour)
    load_data(transformed_transactions, "transactions.json.gz", date, hour)

    # Log processed customers
    customer_ids = [customer["id"] for customer in anonymized_customers]
    log_processed_customers(connection, date, hour, customer_ids)

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        # print("Processing dataset:", dataset_type, "Path:", dataset_path)
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def cleanup_empty_directories(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logger.debug(f"Empty directory deleted: {dir_path}")


def process_all_data():
    connection = connect_to_postgres()
    create_processed_data_log_table(connection)
    # Get a sorted list of date folders
    try:
        date_folders = os.listdir(RAW_DATA_PATH)
        date_folders.sort()
        # Process all available raw_data
        for date_folder in date_folders:
            date_path = os.path.join(RAW_DATA_PATH, date_folder)

            # Get a sorted list of hour folders
            hour_folders = os.listdir(date_path)
            hour_folders.sort()

            for hour_folder in hour_folders:
                hour_path = os.path.join(date_path, hour_folder)

                available_datasets = [filename for filename in os.listdir(hour_path) if filename.endswith((".json", ".json.gz"))]

                if available_datasets:
                    process_hourly_data(connection, date_folder, hour_folder, available_datasets)
                else:
                    logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

                erasure_requests_path = os.path.join(hour_path, "erasure-requests.json.gz")

                # Check if the erasure requests file exists
                if os.path.exists(erasure_requests_path):
                    # Process erasure requests
                    erasure_requests_data = extract_data(erasure_requests_path)
                    # process_erasure_requests(connection, erasure_requests_data)
                else:
                    logger.debug(f"No erasure-requests file found for {date_folder}/{hour_folder}")

        # Clean up empty directories in raw_data after processing
        cleanup_empty_directories(RAW_DATA_PATH)
    finally:
        release_connection(connection)


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
