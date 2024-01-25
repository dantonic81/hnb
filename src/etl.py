# src/etl.py

from data_processing import process_data
from logger import setup_logger


def main():
    logger = setup_logger()
    data_path = "/path/to/data"  # Update with your actual data path

    try:
        process_data(data_path, logger)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
