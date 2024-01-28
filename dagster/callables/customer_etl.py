import logging
import os
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

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


def get_connection():
    return connection_pool.getconn()


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


def process_all_data():
    schema_name = "data"
    connection = connect_to_postgres()
    create_processed_data_log_table(connection, schema_name)


def main():
    try:
        process_all_data()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
