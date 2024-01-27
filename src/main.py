# Import necessary libraries
import logging
import os
import json
import gzip
import hashlib
from datetime import datetime
from dotenv import load_dotenv

import psycopg2

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'raw_data')
PROCESSED_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'processed_data')
ARCHIVED_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'archived_data')


# Helper functions
# TODO schema validation json
# TODO prevent duplicate loading
# TODO handle missing fields and duplicate skus in transformations
# TODO add orchestration / workflows for different arrivals
# TODO add tests
# TODO connect everything to minio
# TODO log records in database for erasure requests and have single source of truth


# Connect to PostgreSQL
def connect_to_postgres():
    connection = psycopg2.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["DB_HOST"],
        database=os.environ["POSTGRES_DB"],
    )
    return connection


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
    # Create the corresponding subdirectories in processed_data
    output_dir = os.path.join(PROCESSED_DATA_PATH, date, hour)
    os.makedirs(output_dir, exist_ok=True)

    # Load processed data to the new location
    output_path = os.path.join(output_dir, f"{dataset_type}.json")
    with open(output_path, "a") as file:
        for record in data:
            json.dump(record, file)
            file.write("\n")


def archive_and_delete(file_path, dataset_type, date, hour, archive_path):
    archive_file = dataset_type
    archive_file_path = os.path.join(archive_path, date, hour, archive_file)

    # Create the archive directory if it doesn't exist
    os.makedirs(os.path.dirname(archive_file_path), exist_ok=True)

    # Archive the file
    os.rename(file_path, archive_file_path)
    logger.info(f"File archived: {archive_file_path}")

    print(f"File path is {file_path}")
    # if dataset_type == "erasure-requests.json":
    #     os.remove(file_path)
    #     logger.info(f"Original file deleted: {file_path}")


def process_hourly_data(date, hour, available_datasets):
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    logger.debug("Dataset Paths:", dataset_paths)  # Debug statement

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
    load_data(anonymized_customers, "customers", date, hour)
    load_data(transformed_products, "products", date, hour)
    load_data(transformed_transactions, "transactions", date, hour)

    # Log processed customers
    customer_ids = [customer["id"] for customer in anonymized_customers]
    connection = connect_to_postgres()
    create_processed_data_log_table(connection)
    log_processed_customers(connection, date, hour, customer_ids)

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        print("Processing dataset:", dataset_type, "Path:", dataset_path)

        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)


def cleanup_empty_directories(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                logger.info(f"Empty directory deleted: {dir_path}")


def process_all_data():
    # Get a sorted list of date folders
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

            available_datasets = [filename for filename in os.listdir(hour_path) if filename.endswith(".json.gz")]

            if available_datasets:
                process_hourly_data(date_folder, hour_folder, available_datasets)
            else:
                logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

    # Clean up empty directories in raw_data after processing
    cleanup_empty_directories(RAW_DATA_PATH)


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
