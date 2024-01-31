import traceback
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import archive_and_delete, extract_actual_date, extract_actual_hour, connect_to_postgres, extract_data, load_data, log_processing_statistics, cleanup_empty_directories
import psycopg2
import cProfile


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DATA_PATH = '/opt/dagster/app/raw_data'
PROCESSED_DATA_PATH = '/opt/dagster/app/processed_data'
ARCHIVED_DATA_PATH = '/opt/dagster/app/archived_data'
INVALID_RECORDS_TABLE = 'data.invalid_transactions'
TRANSACTIONS_SCHEMA_FILE = "transactions_schema.json"


# Load the JSON schema once and store it in a variable
with open(TRANSACTIONS_SCHEMA_FILE, "r") as schema_file:
    TRANSACTIONS_SCHEMA = json.load(schema_file)


def is_existing_customer(connection, customer_id):
    with connection.cursor() as cursor:
        # Check if the customer_id exists in the customer dataset
        cursor.execute("""
            SELECT COUNT(*) FROM data.customers
            WHERE id = %s;
        """, (customer_id,))

        count = cursor.fetchone()[0]

    return count > 0


def is_existing_product(connection, sku):
    with connection.cursor() as cursor:
        # Check if the sku exists in the product dataset
        cursor.execute("""
            SELECT COUNT(*) FROM data.products
            WHERE sku = %s;
        """, (sku,))

        count = cursor.fetchone()[0]

    return count > 0


def are_valid_product_skus(connection, products):
    for product in products:
        sku = product.get('sku')
        if not is_existing_product(connection, sku):
            return False
    return True


def is_valid_total_cost(products, total_cost):
    # Calculate the total cost based on individual product prices and quantities
    calculated_total_cost = sum(float(product.get('price', 0)) * float(product.get('quanitity', 0)) for product in products)
    # Compare the calculated total cost with the provided total_cost
    return round(calculated_total_cost, 2) == round(float(total_cost), 2)


def bulk_insert_invalid_transactions(connection, invalid_transactions):
    with connection.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO data.invalid_transactions (record_date, record_hour, transaction_id, customer_id, error_message) 
            VALUES (%s, %s, %s, %s, %s);
        """, [(extract_actual_date(date), extract_actual_hour(hour), t.get("transaction_id"), t.get("customer_id"), error_message)
              for t, error_message, date, hour in invalid_transactions])
    connection.commit()


def transform_and_validate_transactions(connection, transactions_data, date, hour):
    schema = TRANSACTIONS_SCHEMA

    valid_transactions = []
    invalid_transactions = []
    unique_transaction_ids = set()

    # Validate each transaction record against the schema
    for transaction in transactions_data:
        try:
            validate(instance=transaction, schema=schema)

            # Check uniqueness of transaction_id
            transaction_id = transaction.get('transaction_id')
            if transaction_id in unique_transaction_ids:
                # Log or handle duplicate transaction_id
                logger.debug(f"Duplicate transaction_id found: {transaction_id}")
                invalid_transactions.append((transaction, "Duplicate transaction_id", date, hour))
                continue

            unique_transaction_ids.add(transaction_id)

            # Check if customer_id refers to an existing customer
            customer_id = transaction.get('customer_id')
            if not is_existing_customer(connection, customer_id):
                # Log or handle invalid customer_id
                logger.debug(f"Invalid customer_id found: {customer_id}")
                invalid_transactions.append((transaction, f"Invalid customer_id: {customer_id}", date, hour))
                continue

            # Check if product skus correspond to existing products
            if not are_valid_product_skus(connection, transaction.get('purchases', {}).get('products', [])):
                # Log or handle invalid product skus
                logger.debug(f"Invalid product skus found in transaction_id: {transaction_id}")
                invalid_transactions.append((transaction, "Invalid product skus", date, hour))
                continue

            # Check if total_cost matches the sum of individual product costs
            purchases = transaction.get('purchases', {})
            if not is_valid_total_cost(purchases.get('products', []), purchases.get('total_cost')):
                # Log or handle invalid total_cost
                logger.debug(f"Invalid total_cost found in transaction_id: {transaction_id}")
                invalid_transactions.append((transaction, "Invalid total_cost", date, hour))
                continue

            valid_transactions.append(transaction)

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for transaction: {e}")
            invalid_transactions.append((transaction, str(e), date, hour))
            continue

    # Bulk insert invalid transactions
    bulk_insert_invalid_transactions(connection, invalid_transactions)

    return valid_transactions


def get_delivery_address_by_transaction_id(connection, transaction_id):
    """
    Retrieve delivery address information for a given transaction_id.

    Parameters:
    - connection: Database connection object
    - transaction_id: ID of the transaction

    Returns:
    - Dictionary containing delivery address information
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT address, postcode, city, country
            FROM data.delivery_addresses
            WHERE transaction_id = %s;
        """, (transaction_id,))

        result = cursor.fetchone()

    return {
        'address': result[0],
        'postcode': result[1],
        'city': result[2],
        'country': result[3]
    } if result else None


def get_purchases_by_transaction_id(connection, transaction_id):
    """
    Retrieve purchase information for a given transaction_id.

    Parameters:
    - connection: Database connection object
    - transaction_id: ID of the transaction

    Returns:
    - List of dictionaries containing purchase information
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT product_sku, quantity, price, total
            FROM data.purchases
            WHERE transaction_id = %s;
        """, (transaction_id,))

        results = cursor.fetchall()

    # Convert the results to a list of dictionaries
    purchases = []
    for result in results:
        purchases.append({
            'product_sku': result[0],
            'quantity': result[1],
            'price': result[2],
            'total': result[3]
        })

    return purchases if purchases else None


def log_processed_transactions(connection, date, hour, transaction_ids, transaction_times, customer_ids):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for transaction_id, transaction_time, customer_id in zip(transaction_ids, transaction_times, customer_ids):
            # Check if the record already exists
            cursor.execute("""
                SELECT COUNT(*) FROM data.transactions
                WHERE record_date = %s AND record_hour = %s AND transaction_id = %s;
            """, (actual_date, actual_hour, transaction_id))

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute("""
                    INSERT INTO data.transactions (record_date, record_hour, transaction_id, transaction_time, customer_id) 
                    VALUES (%s, %s, %s, %s, %s);
                """, (actual_date, actual_hour, transaction_id, transaction_time, customer_id))

                # Insert delivery_address into delivery_addresses table
                delivery_address = get_delivery_address_by_transaction_id(connection, transaction_id)
                if delivery_address:
                    cursor.execute("""
                        INSERT INTO data.delivery_addresses (transaction_id, address, postcode, city, country)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (transaction_id, delivery_address['address'], delivery_address['postcode'], delivery_address['city'],
                          delivery_address['country']))

                purchases = get_purchases_by_transaction_id(connection, transaction_id)
                # Check if purchases is not None before iterating
                if purchases is not None:
                    for purchase in purchases:
                        cursor.execute("""
                            INSERT INTO data.purchases (transaction_id, product_sku, quantity, price, total)
                            VALUES (%s, %s, %s, %s, %s);
                        """, (transaction_id, purchase['sku'], purchase['quantity'], purchase['price'], purchase['total']))

            else:
                # Record already exists, log or handle accordingly
                logger.info(f"Record for transaction_id {transaction_id} at {actual_date} {actual_hour} and customer id {customer_id} already exists.")
    connection.commit()


def process_hourly_data(connection, date, hour, available_datasets):
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    logger.debug("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract raw_data
    transactions_data = extract_data(dataset_paths.get("transactions.json.gz", ""))

    # Transform and validate raw_data
    transformed_transactions = transform_and_validate_transactions(connection, transactions_data, date, hour)

    # Load processed raw_data
    load_data(transformed_transactions, "transactions.json.gz", date, hour, PROCESSED_DATA_PATH)

    # Log processed transactions
    transaction_ids = [transaction["transaction_id"] for transaction in transformed_transactions]
    customer_ids = [transaction["customer_id"] for transaction in transformed_transactions]
    transaction_times = [transaction["transaction_time"] for transaction in transformed_transactions]
    log_processed_transactions(connection, date, hour, transaction_ids, transaction_times, customer_ids)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(connection, date, hour, "transactions.json.gz", len(transformed_transactions), processing_time)

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        archive_and_delete(dataset_path, dataset_type, date, hour, ARCHIVED_DATA_PATH)
    logger.debug("Processing completed.")


def process_all_data():
    connection = connect_to_postgres()
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

                available_datasets = [filename for filename in os.listdir(hour_path) if filename.startswith("transactions")
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
    cProfile.run('main()', sort='cumulative')
