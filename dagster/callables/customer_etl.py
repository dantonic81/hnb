import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_all_data():
    pass


def main():
    try:
        process_all_data()
        logger.info("Hello from customer_etl.py!")
        return 222333
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
