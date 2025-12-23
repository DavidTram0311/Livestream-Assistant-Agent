import os

from dotenv import load_dotenv
import logging
from db.postgres_client import PostgresSQLClient

load_dotenv()

# ADD THIS: Configure logging to show INFO level messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("Initializing PostgreSQL client")
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    try:
        # Drop all tables
        logging.info("Dropping existing tables")
        pc.drop_tables()

        # Create tables
        logging.info("Creating tables")
        pc.create_tables()

        logging.info("✅ Successfully created events table")
    except Exception as e:
        logging.error(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
