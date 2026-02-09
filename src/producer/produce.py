"""Refactored producer using shared utilities"""
import time
import argparse
from pathlib import Path
from common.logging import setup_logging, get_logger
from common.kafka import KafkaProducerClient, KafkaAdminClient
from common.storage import ParquetBatchReader
from .config import ProducerConfig

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with comment events."
)
parser.add_argument(
    "-t",
    "--type",
    default="local",
    choices=["local", "containers"],
    help="The type of Kafka bootstrap servers to use."
)


def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        logger.error(f"Error producing record {msg.key()}: {err}")
    else:
        logger.info(
            f"Record {msg.key()} successfully produced to "
            f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def produce_comment_events(config: ProducerConfig, kafka_mode: str):
    """
    Produce comment events to Kafka using shared utilities.
    
    Args:
        config: ProducerConfig instance
        kafka_mode: 'local' or 'containers'
    """
    base_dir = Path(__file__).parent
    bootstrap_servers = config.get_bootstrap_servers(kafka_mode)
    schema_path = config.get_absolute_schema_path(str(base_dir))
    parquet_path = config.get_absolute_parquet_path(str(base_dir))
    
    logger.info(f"Using Kafka bootstrap servers: {bootstrap_servers}")
    logger.info(f"Schema path: {schema_path}")
    logger.info(f"Parquet path: {parquet_path}")
    
    # Initialize Kafka Admin Client
    admin = KafkaAdminClient(bootstrap_servers)
    
    # Create topic
    logger.info(f"Creating topic: {config.output_topic}")
    admin.create_topic(
        topic_name=config.output_topic,
        num_partitions=config.num_partitions,
        replication_factor=config.replication_factor
    )
    
    # Initialize Kafka Producer
    logger.info("Initializing Kafka producer...")
    producer = KafkaProducerClient(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=config.schema_registry_url,
        avro_schema_path=schema_path
    )
    
    # Initialize Parquet Reader
    logger.info(f"Reading parquet file: {parquet_path}")
    reader = ParquetBatchReader(parquet_path, batch_size=config.batch_size)
    
    # Produce records
    try:
        logger.info("Starting to send records...")
        comment_id = 0
        
        for batch_df in reader.iter_batches():
            for _, row in batch_df.iterrows():
                comment_id += 1
                record = {
                    "comment_id": int(comment_id),
                    "user_id": str(row.reviewerID),
                    "comments": str(row.reviewText),
                    "event_timestamp": int(time.time() * 1000)
                }
                
                producer.produce(
                    topic=config.output_topic,
                    key=record["user_id"],
                    value=record,
                    on_delivery=delivery_report
                )
        
        logger.info("All records sent successfully")
        producer.flush()
        logger.info("Producer flushed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error producing records: {e}")
        return False


def teardown_topic(config: ProducerConfig, kafka_mode: str):
    """
    Delete Kafka topic.
    
    Args:
        config: ProducerConfig instance
        kafka_mode: 'local' or 'containers'
    """
    bootstrap_servers = config.get_bootstrap_servers(kafka_mode)
    admin = KafkaAdminClient(bootstrap_servers)
    
    logger.info(f"Deleting topic: {config.output_topic}")
    admin.delete_topic(config.output_topic)


def main():
    """Main entry point"""
    args = parser.parse_args()
    
    # Load configuration
    config = ProducerConfig()
    
    if args.mode == "setup":
        # Teardown first if setup is specified
        teardown_topic(config, args.type)
        time.sleep(5)
        produce_comment_events(config, args.type)
        
    elif args.mode == "teardown":
        teardown_topic(config, args.type)


if __name__ == "__main__":
    main()
