# Import necessary libraries
import os
import json
import gzip
import hashlib
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'raw_data')
PROCESSED_DATA_PATH = os.path.join(SCRIPT_DIR, '..', 'processed_data')


# Helper functions
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


def transform_and_validate_transactions(transactions_data, customers_data, products_data):
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


def load_data(data, dataset_type):
    # Load processed raw_data to the new location
    output_path = os.path.join(PROCESSED_DATA_PATH, f"{dataset_type}.json")
    with open(output_path, "a") as file:
        for record in data:
            json.dump(record, file)
            file.write("\n")


def process_hourly_data(date, hour, available_datasets):
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    print("Dataset Paths:", dataset_paths)  # Debug statement

    # Extract raw_data
    customers_data = extract_data(dataset_paths.get("customers.json.gz", ""))
    products_data = extract_data(dataset_paths.get("products.json.gz", ""))
    transactions_data = extract_data(dataset_paths.get("transactions.json.gz", ""))
    erasure_requests_data = extract_data(dataset_paths.get("erasure-requests.json.gz", ""))

    # Transform and validate raw_data
    transformed_customers = transform_and_validate_customers(customers_data)
    transformed_products = transform_and_validate_products(products_data)
    transformed_transactions = transform_and_validate_transactions(transactions_data, transformed_customers,
                                                                   transformed_products)

    # Anonymize customer raw_data
    anonymized_customers = anonymize_customer_data(transformed_customers, erasure_requests_data)

    # Load processed raw_data
    load_data(anonymized_customers, "customers")
    load_data(transformed_products, "products")
    load_data(transformed_transactions, "transactions")


def process_all_data(logger):
    # Process all available raw_data
    for date_folder in os.listdir(RAW_DATA_PATH):
        date_path = os.path.join(RAW_DATA_PATH, date_folder)
        for hour_folder in os.listdir(date_path):
            hour_path = os.path.join(date_path, hour_folder)

            available_datasets = [filename for filename in os.listdir(hour_path) if filename.endswith(".json.gz")]

            if available_datasets:
                process_hourly_data(date_folder, hour_folder, available_datasets)
            else:
                print(f"Warning: No datasets found for {date_folder}/{hour_folder}")


if __name__ == "__main__":
    from logger import setup_logger
    logger = setup_logger()
    process_all_data(logger)

