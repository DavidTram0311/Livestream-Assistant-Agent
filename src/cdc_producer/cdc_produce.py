import os
import logging
import time
from db import PostgresSQLClient
from db import Event
import pandas as pd
import pyarrow.parquet as pq
import random
from dotenv import load_dotenv
import argparse
from sqlalchemy import func

# Add logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

parser = argparse.ArgumentParser()
parser.add_argument(
    "-b",
    "--batch_size",
    type=int,
    default=10,
    help="The batch size of the events to produce"
)

load_dotenv()

REVIEW_PARQUET_PATH = os.path.join(os.path.dirname(__file__), "data", "user_comments.parquet")


def format_event(row):
    return Event(
        user_id=str(row.get("user_id", "anonymous")),
        comments=str(row.get("comments", ""))
        )

def produce_event(review_parquet_path: str, batch_size: int):
    """
    Produce events from a parquet file randomly and waiting for a random time between 1 and 10 seconds.

    Args:
        review_parquet_path: The path to the parquet file.
    """
    # Connect to PostgreSQL Client
    logging.info("Connecting to PostgreSQL Client")
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host="localhost",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    logging.info("PostgreSQL Client connected successfully")

    # Get session
    session = pc.get_session()

    # Produce events to PostgreSQL Database
    logging.info("Producing events to PostgreSQL Database... 🔥")

    try: 
        parquet_file = pq.ParquetFile(review_parquet_path)
        total_processed = 0

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            records = []
            batch_df = batch.to_pandas()

            print(f"Batch length: {len(batch_df)}")

            for index, row in batch_df.iterrows():
                random_stop = random.randint(1, batch_size)
                record = format_event(
                    {
                        "user_id": row["reviewerID"],
                        "comments": row["reviewText"]
                    }
                )
                records.append(record)
                total_processed += 1

                if random_stop == index:
                    break

            try:
                session.bulk_save_objects(records)
                session.commit()
            except Exception as e:
                session.rollback()
                logging.error(f"Failed to commit batchs to PostgreSQL Database: {e}")

            logging.info(f"Processed {len(records)} records")
            time.sleep(random.uniform(1, 10))
        
            if total_processed >= 100:
                logging.info(f"Total processed records reached 100, breaking the loop")
                break
    
    except Exception as e:
        logging.error(f"Error producing events to PostgreSQL Database: {e}")
        session.rollback()
        raise e

    finally:
        session.close()
        logging.info("PostgreSQL Client disconnected successfully")
        logging.info(f"Processed {total_processed} records")

if __name__ == "__main__":
    args = parser.parse_args()

    produce_event(REVIEW_PARQUET_PATH, args.batch_size)