import traceback
import gzip
import json
import hashlib
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import jsonschema
from jsonschema import validate
from common import connect_to_postgres, cleanup_empty_directories, archive_and_delete, extract_actual_date, extract_actual_hour, log_processing_statistics, extract_data
import psycopg2

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = '/opt/dagster/app/raw_data'
PROCESSED_DATA_PATH = '/opt/dagster/app/processed_data'
ARCHIVED_DATA_PATH = '/opt/dagster/app/archived_data'
INVALID_RECORDS_TABLE = 'data.invalid_customers'
ERASURE_REQUESTS_SCHEMA_FILE = "erasure_requests_schema.json"


# Query the customers table to get date and hour for a given customer_id
def get_date_and_hour_to_anonymize(connection, customer_id):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT record_date, record_hour FROM data.customers
            WHERE id = %s
        """, (customer_id,))
        result = cursor.fetchone()
        if result:
            return result
    return None


# Locate the processed data file based on date and hour
def locate_processed_data_file(date, hour):
    formatted_date = format_date_for_file_system(date)
    formatted_hour = format_hour_for_file_system(hour)
    processed_data_path = os.path.join(PROCESSED_DATA_PATH, str(formatted_date), str(formatted_hour))
    for filename in os.listdir(processed_data_path):
        if filename.endswith((".json", ".json.gz")) and filename.startswith("customers"):
            return os.path.join(processed_data_path, filename)
    return None


# Anonymize and update the data in the processed data file
def anonymize_and_update_data(file_path, customer_id, erasure_request):
    email_to_anonymize = erasure_request.get("email")
    anonymized_email = hashlib.sha256(email_to_anonymize.encode()).hexdigest()
    logger.debug(f"email: {email_to_anonymize}, anonymized_email: {anonymized_email}")
    try:
        is_gzipped = file_path.endswith(".gz")

        with (gzip.open(file_path, "rt") if is_gzipped else open(file_path, "r")) as file:
            data = [json.loads(line) for line in file]

        for record in data:
            if record.get("id") == customer_id:
                # Anonymize the email in the record
                record["email"] = anonymized_email

        with (gzip.open(file_path, "wt") if is_gzipped else open(file_path, "w")) as file:
            for record in data:
                json.dump(record, file)
                file.write("\n")
    except Exception as e:
        logger.error(f"An error occurred while updating file {file_path}: {str(e)}")
        traceback.print_exc()


# Archive the updated file
def archive_updated_file(file_path, date, hour):
    archive_path = os.path.join(ARCHIVED_DATA_PATH, str(date), str(hour))
    os.makedirs(archive_path, exist_ok=True)

    archive_file = os.path.basename(file_path)
    archive_file_path = os.path.join(archive_path, archive_file)

    os.rename(file_path, archive_file_path)
    logger.info(f"File archived: {archive_file_path}")


def process_erasure_requests(connection, erasure_requests):
    for erasure_request in erasure_requests:
        customer_id = erasure_request.get("customer-id")
        if customer_id:
            # Step 1: Query the processed_data_log table to get date and hour
            result = get_date_and_hour_to_anonymize(connection, customer_id)
            if result:
                date, hour = result

                # Step 2: Locate the processed data file
                file_path = locate_processed_data_file(date, hour)
                if file_path:
                    # Step 3: Anonymize and update the data
                    anonymize_and_update_data(file_path, customer_id, erasure_request)

                    # Step 4: Archive the updated file
                    archive_updated_file(file_path, date, hour)


def format_date_for_file_system(actual_date):
    return f"date={actual_date.strftime('%Y-%m-%d')}"


def format_hour_for_file_system(actual_hour):
    return f"hour={actual_hour:02}"


def log_invalid_erasure_request(connection, erasure_request, error_message, date, hour):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        # Check if the customer with the same id already exists
        customer_id = erasure_request.get("customer-id")
        cursor.execute("""
            SELECT customer_id FROM data.invalid_erasure_requests WHERE customer_id = %s;
        """, (customer_id,))

        existing_record = cursor.fetchone()

        if existing_record:
            # Update the existing record if needed
            cursor.execute("""
                UPDATE data.invalid_erasure_requests
                SET error_message = %s
                WHERE customer_id = %s;
            """, (error_message, customer_id))
        else:
            cursor.execute("""
                INSERT INTO data.invalid_erasure_requests (record_date, record_hour, customer_id, error_message) 
                VALUES (%s, %s, %s, %s);
            """, (actual_date, actual_hour, customer_id, error_message))
    connection.commit()


def transform_and_validate_erasure_requests(connection, erasure_requests_data, date, hour):
    with open(ERASURE_REQUESTS_SCHEMA_FILE, "r") as schema_file:
        schema = json.load(schema_file)

    valid_erasure_requests = []

    # Keep track of unique customer-ids
    unique_customer_ids = set()

    # Validate each erasure request against the schema
    for erasure_request in erasure_requests_data:
        try:
            # Perform validation
            validate(instance=erasure_request, schema=schema)

            # Extract customer-id from the erasure request
            customer_id = erasure_request.get("customer-id")

            # Check uniqueness of customer-id
            if customer_id and customer_id not in unique_customer_ids:
                unique_customer_ids.add(customer_id)
                valid_erasure_requests.append(erasure_request)
            else:
                # Log or handle duplicate customer-id
                logger.debug(f"Duplicate customer-id found for erasure request: {customer_id}")
                log_invalid_erasure_request(connection, erasure_request, "Duplicate customer-id", date, hour)

        except jsonschema.exceptions.ValidationError as e:
            # Log or handle validation errors
            logger.error(f"Validation error for erasure request: {e}")
            log_invalid_erasure_request(connection, erasure_request, str(e), date, hour)
            continue

    return valid_erasure_requests


def log_processed_erasure_requests(connection, date, hour, customer_ids, emails):
    actual_date = extract_actual_date(date)
    actual_hour = extract_actual_hour(hour)
    with connection.cursor() as cursor:
        for customer_id, email in zip(customer_ids, emails):
            # Check if the record already exists
            cursor.execute("""
                SELECT COUNT(*) FROM data.erasure_requests 
                WHERE record_date = %s AND record_hour = %s AND customer_id = %s;
            """, (actual_date, actual_hour, customer_id))

            count = cursor.fetchone()[0]
            if count == 0:
                # Record doesn't exist, insert it
                cursor.execute("""
                    INSERT INTO data.erasure_requests (record_date, record_hour, customer_id, email) 
                    VALUES (%s, %s, %s, %s);
                """, (actual_date, actual_hour, customer_id, email))
            else:
                # Record already exists, log or handle accordingly
                logger.info(f"Record for customer_id {customer_id} at {actual_date} {actual_hour} already exists.")
    connection.commit()


def process_hourly_data(connection, date, hour, available_datasets):
    dataset_paths = {dataset: os.path.join(RAW_DATA_PATH, f"{date}", f"{hour}", f"{dataset}")
                     for dataset in available_datasets}
    logger.debug("Dataset Paths:", dataset_paths)

    # Record the start time
    start_time = datetime.now()

    # Extract raw_data from both .json.gz and .json files
    erasure_requests_data = []
    for dataset_type in ["erasure-requests.json.gz", "erasure-requests.json"]:
        if dataset_type in dataset_paths:
            erasure_requests_data.extend(extract_data(dataset_paths[dataset_type]))

    transformed_and_validated_erasure_requests = transform_and_validate_erasure_requests(connection, erasure_requests_data, date, hour)

    process_erasure_requests(connection, erasure_requests_data)
    customer_ids = [request["customer-id"] for request in transformed_and_validated_erasure_requests]
    emails = [request["email"] for request in transformed_and_validated_erasure_requests]
    log_processed_erasure_requests(connection, date, hour, customer_ids, emails)

    # Record the end time
    end_time = datetime.now()

    # Calculate processing time
    processing_time = end_time - start_time
    log_processing_statistics(connection, date, hour, "erasure_requests.json.gz", len(erasure_requests_data), processing_time)

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

                available_datasets = [filename for filename in os.listdir(hour_path) if filename.startswith("erasure")
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
