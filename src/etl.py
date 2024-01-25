# src/etl.py

from data_processing import process_all_data
from logger import setup_logger


def main():
    logger = setup_logger()
    try:
        process_all_data(logger)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
