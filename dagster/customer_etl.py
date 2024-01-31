import traceback
import gzip
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
import jsonschema
from jsonschema import validate

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = '/opt/dagster/app/raw_data'
PROCESSED_DATA_PATH = '/opt/dagster/app/processed_data'
ARCHIVED_DATA_PATH = '/opt/dagster/app/archived_data'
INVALID_RECORDS_TABLE = 'data.invalid_customers'
CUSTOMERS_SCHEMA_FILE = "customer_schema.json"


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


connection_pool = create_connection_pool()


def get_connection():
    return connection_pool.getconn()


def release_connection(conn):
    connection_pool.putconn(conn)


def connect_to_postgres():
    return get_connection()


def log_invalid_customer(connection, customer, error_message, date, hour):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        # Check if the customer with the same id already exists
        cursor.execute("""
            SELECT id FROM data.invalid_customers WHERE id = %s;
        """, (customer.get("id"),))

        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record if needed
            cursor.execute("""
                UPDATE data.invalid_customers
                SET error_message = %s
                WHERE id = %s;
            """, (error_message, customer.get("id")))
        else:
            cursor.execute("""
                INSERT INTO data.invalid_customers (record_date, record_hour, id, first_name, last_name, email, error_message) 
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (actual_date, actual_hour, customer.get("id"), customer.get("first_name"), customer.get("last_name"), customer.get("email"), error_message))
    connection.commit()


def extract_data(file_path):
    if not file_path:
        return []

    # Extract raw_data from a gzipped JSON file
    with gzip.open(file_path, "rt") as file:
        data = [json.loads(line) for line in file]
    return data


def log_processing_statistics(connection, date, hour, dataset_type, record_count, processing_time):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO data.processing_statistics (record_date, record_hour, dataset_type, record_count, processing_time) 
            VALUES (%s, %s, %s, %s, %s);
        """, (actual_date, actual_hour, dataset_type, record_count, processing_time))
    connection.commit()


def transform_and_validate_customers(connection, customers_data, date, hour):
    # Load the JSON schema
    with open(CUSTOMERS_SCHEMA_FILE, "r") as schema_file:
        schema = json.load(schema_file)

    valid_customers = []

    # Keep track of unique ids
    unique_ids = set()

    # Validate each customer record against the schema
    for customer in customers_data:
        try:
            validate(instance=customer, schema=schema)

            # Convert 'id' to integer
            customer_id = int(customer['id'])

            # Check uniqueness of id
            if customer_id not in unique_ids:
                unique_ids.add(customer_id)
                valid_customers.append(customer)
            else:
                # Log or handle duplicate id
                print(f"Duplicate id found for customer: {customer['id']}")
                log_invalid_customer(connection, customer, "Duplicate id", date, hour)

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            print(f"Validation error for customer: {e}")
            log_invalid_customer(connection, customer, str(e), date, hour)
            continue

    # Update last_change timestamp
    for customer in valid_customers:
        customer['last_change'] = datetime.utcnow().isoformat()

    return valid_customers


def extract_actual_date(date_str):
    return datetime.strptime(date_str.replace("date=", ""), "%Y-%m-%d").date()


# Extract the actual hour from the "hour=00" format
def extract_actual_hour(hour_str):
    return int(hour_str.replace("hour=", ""))


def log_processed_customers(connection, date, hour, customer_ids, first_names, last_names, emails):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for customer_id, first_name, last_name, email in zip(customer_ids, first_names, last_names, emails):
            # Check if the record already exists
            cursor.execute("""
                SELECT COUNT(*) FROM data.customers 
                WHERE record_date = %s AND record_hour = %s AND id = %s;
            """, (actual_date, actual_hour, customer_id))

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute("""
                    INSERT INTO data.customers (record_date, record_hour, id, first_name, last_name, email) 
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (actual_date, actual_hour, customer_id, first_name, last_name, email))
            else:
                # Record already exists, log or handle accordingly
                logger.info(f"Record for customer_id {customer_id} at {actual_date} {actual_hour} already exists.")
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

    # Record the start time
    start_time = datetime.now()

    # Extract raw_data
    customers_data = extract_data(dataset_paths.get("customers.json.gz", ""))
    print("Number of customers:", len(customers_data))

    # Transform and validate raw_data
    transformed_customers = transform_and_validate_customers(connection, customers_data, date, hour)

    # Load processed raw_data
    load_data(transformed_customers, "customers.json.gz", date, hour)

    # Log processed customers
    customer_ids = [customer["id"] for customer in transformed_customers]
    first_names = [customer["first_name"] for customer in transformed_customers]
    last_names = [customer["last_name"] for customer in transformed_customers]
    emails = [customer["email"] for customer in transformed_customers]
    log_processed_customers(connection, date, hour, customer_ids, first_names, last_names, emails)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(connection, date, hour, "customers.json.gz", len(transformed_customers), processing_time)

    # Archive and delete the original files
    for dataset_type, dataset_path in dataset_paths.items():
        print("Processing dataset:", dataset_type, "Path:", dataset_path)
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

        # Clean up empty directories in raw_data after processing
        cleanup_empty_directories(RAW_DATA_PATH)
    finally:
        release_connection(connection)


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
