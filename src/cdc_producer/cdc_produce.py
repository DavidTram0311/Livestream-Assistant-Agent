"""Refactored CDC producer using shared utilities"""
import time
import random
import argparse
from pathlib import Path
from common.logging import setup_logging, get_logger
from common.storage import ParquetBatchReader
from common.db import PostgresClient
from .config import CDCProducerConfig
from .db import Event

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "-b",
    "--batch_size",
    type=int,
    default=10,
    help="The batch size of the events to produce"
)


def format_event(row):
    """Format row data into Event object"""
    return Event(
        user_id=str(row.get("user_id", "anonymous")),
        comments=str(row.get("comments", ""))
    )


def produce_event(config: CDCProducerConfig):
    """
    Produce events from parquet file to PostgreSQL.
    
    Args:
        config: CDCProducerConfig instance
    """
    base_dir = Path(__file__).parent
    parquet_path = config.get_absolute_parquet_path(str(base_dir))
    
    logger.info(f"Parquet path: {parquet_path}")
    
    # Initialize PostgreSQL client using shared utility
    logger.info("Connecting to PostgreSQL Client")
    pg_client = PostgresClient(config)
    logger.info("PostgreSQL Client connected successfully")
    
    # Initialize Parquet Reader
    logger.info(f"Reading parquet file: {parquet_path}")
    reader = ParquetBatchReader(parquet_path, batch_size=config.batch_size)
    
    # Produce events to PostgreSQL Database
    logger.info("Producing events to PostgreSQL Database... 🔥")
    
    total_processed = 0
    
    try:
        with pg_client.get_session() as session:
            for batch_df in reader.iter_batches(max_records=config.max_records):
                records = []
                
                logger.info(f"Batch length: {len(batch_df)}")
                
                for index, row in batch_df.iterrows():
                    random_stop = random.randint(1, config.batch_size)
                    
                    record = format_event({
                        "user_id": row["reviewerID"],
                        "comments": row["reviewText"]
                    })
                    records.append(record)
                    total_processed += 1
                    
                    if random_stop == index:
                        break
                
                # Bulk insert
                try:
                    session.bulk_save_objects(records)
                    session.commit()
                    logger.info(f"Processed {len(records)} records")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to commit batch to PostgreSQL: {e}")
                    raise
                
                # Random sleep between batches
                time.sleep(random.uniform(1, 10))
                
                if total_processed >= config.max_records:
                    logger.info(f"Total processed records reached {config.max_records}, breaking")
                    break
    
    except Exception as e:
        logger.error(f"Error producing events to PostgreSQL Database: {e}")
        raise
    
    finally:
        pg_client.close()
        logger.info("PostgreSQL Client disconnected successfully")
        logger.info(f"Total processed: {total_processed} records")


def main():
    """Main entry point"""
    args = parser.parse_args()
    
    # Load configuration
    config = CDCProducerConfig()
    config.batch_size = args.batch_size
    
    produce_event(config)


if __name__ == "__main__":
    main()