import traceback
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import connect_to_postgres, cleanup_empty_directories, archive_and_delete, log_processing_statistics, extract_data, load_data, extract_actual_date, extract_actual_hour
import psycopg2


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = '/opt/dagster/app/raw_data'
PROCESSED_DATA_PATH = '/opt/dagster/app/processed_data'
ARCHIVED_DATA_PATH = '/opt/dagster/app/archived_data'
INVALID_RECORDS_TABLE = 'data.invalid_products'
PRODUCTS_SCHEMA_FILE = "products_schema.json"


def log_invalid_product(connection, product, error_message, date, hour):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO data.invalid_products (record_date, record_hour, sku, name, price, category, popularity, error_message) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (actual_date, actual_hour, product.get("sku"), product.get("name"), product.get("price"), product.get("category"), product.get("popularity"), error_message))
    connection.commit()


def transform_and_validate_products(connection, products_data, date, hour):
    # Load the JSON schema
    with open("products_schema.json", "r") as schema_file:
        schema = json.load(schema_file)

    valid_products = []

    # Validate each product record against the schema
    for product in products_data:
        try:
            # Convert 'price' to a number before validation
            product['price'] = float(product['price'])
            validate(instance=product, schema=schema)
            valid_products.append(product)
        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            print(f"Validation error for product: {e}")
            # Log the invalid record to the database
            log_invalid_product(connection, product, str(e), date, hour)
            continue

    # Update last_change timestamp
    for product in valid_products:
        product['last_change'] = datetime.utcnow().isoformat()

    return valid_products


def log_processed_products(connection, date, hour, skus, names, prices, categories, popularities):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for sku, name, price, category, popularity in zip(skus, names, prices, categories, popularities):
            # Check if the record already exists
            cursor.execute("""
                SELECT COUNT(*) FROM data.products
                WHERE record_date = %s AND record_hour = %s AND sku = %s;
            """, (actual_date, actual_hour, sku))

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute("""
                    INSERT INTO data.products (sku, name, price, category, popularity, record_date, record_hour)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (sku, name, price, category, popularity, actual_date, actual_hour))
            else:
                # Record already exists, log or handle accordingly
                logger.info(f"Record for sku {sku} at {actual_date} {actual_hour} already exists.")
    connection.commit()


def process_hourly_data(connection, date, hour, available_datasets):
    print(date, hour, len(available_datasets), available_datasets)
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    print("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract raw_data
    products_data = extract_data(dataset_paths.get("products.json.gz", ""))
    print("Number of products:", len(products_data))

    # Transform and validate raw_data
    transformed_products = transform_and_validate_products(connection, products_data, date, hour)

    # Load processed raw_data
    load_data(transformed_products, "products.json.gz", date, hour, PROCESSED_DATA_PATH)

    # Log processed products
    skus = [product["sku"] for product in transformed_products]
    names = [product["name"] for product in transformed_products]
    prices = [product["price"] for product in transformed_products]
    categories = [product["category"] for product in transformed_products]
    popularities = [product["popularity"] for product in transformed_products]
    log_processed_products(connection, date, hour, skus, names, prices, categories, popularities)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(connection, date, hour, "products.json.gz", len(transformed_products), processing_time)

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        print("Processing dataset:", dataset_type, "Path:", dataset_path)
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data():
    connection = connect_to_postgres()
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

                available_datasets = [filename for filename in os.listdir(hour_path) if filename.startswith("products")
                                      and filename.endswith((".json", ".json.gz"))]

                if available_datasets:
                    process_hourly_data(connection, date_folder, hour_folder, available_datasets)
                else:
                    logger.warning(f"No datasets found for {date_folder}/{hour_folder}")

        # Clean up empty directories in raw_data after processing
        cleanup_empty_directories(RAW_DATA_PATH)

    except (psycopg2.Error, Exception) as e:
        logger.error(f"An error occurred while processing data: {str(e)}")
        traceback.print_exc()


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
