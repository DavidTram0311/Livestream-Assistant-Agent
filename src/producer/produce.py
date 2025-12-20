import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os
import logging
import pandas as pd
import argparse
import json
import pyarrow.parquet as pq
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_SERVER")
TOPIC_NAME = os.getenv("KAFKA_OUTPUT_TOPICS")
AVRO_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "avro_schemas", "comment_events.avsc")
REVIEW_PARQUET_PATH = os.path.join(os.path.dirname(__file__), "data", "user_table.parquet")


parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with comment events. Setup will teardown before beginning emitting events."
)

def create_topic(topic_name: str, num_partitions: int, replication_factor: int, kafka_bootstrap: str):
    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap})
    new_topic = NewTopic(topic_name, num_partitions, replication_factor)

    # Create topic
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            logging.info(f"Topic {topic} created successfully")
        except Exception as e:
            logging.error(f"Error creating topic {topic}: {e}")

def teardown_topic(topic_name: str, kafka_bootstrap: str):
    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap})
    fs = admin_client.delete_topics([topic_name])

    for topic, f in fs.items():
        try:
            f.result()
            logging.info(f"Topic {topic} deleted successfully")
        except Exception as e:
            logging.error(f"Error deleting topic {topic}: {e}")

def produce_comment_events(
    avro_schema_path: str, 
    schema_registry_url: str, 
    kafka_bootstrap: str,
    review_parquet_path: str,
    topic_name: str,
    num_partitions: int,
    replication_factor: int
):
    # Initialize Schema Registry Client
    # Retry 10 times with a 1 second delay
    for _ in range(10):
        try:
            schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
            break
        except Exception as e:
            logging.error(f"Error initializing Schema Registry Client: {e}")
            time.sleep(1)
    else:
        raise Exception("Failed to initialize Schema Registry Client")

    # Initialize Avro Serializer
    with open(avro_schema_path, "r") as f:
        value_schema_str = f.read()
    avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

    # Initialize Producer
    for _ in range(10):
        try:
            producer_conf = {
                "bootstrap.servers": kafka_bootstrap,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": avro_serializer,
            }
            producer = SerializingProducer(producer_conf)
            logging.info("Producer initialized successfully")
            break
        except Exception as e:
            logging.error(f"Error initializing Producer: {e}")
            time.sleep(1)
    else:
        raise Exception("Failed to initialize Producer")


    # Create topic
    def delivery_report(err, msg):
        if err is not None:
            logging.error(f"Error producing record {msg.key()}: {err}")
        else:
            logging.info(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    create_topic(topic_name, num_partitions, replication_factor, kafka_bootstrap)

    # Produce records in batch
    try:
        parquet_file = pq.ParquetFile(review_parquet_path)
        logging.info("Starting to send records...")

        for batch in parquet_file.iter_batches(batch_size=1000):
            batch_df = batch.to_pandas()

            for index, row in batch_df.iterrows():
                record = {
                    "reviewerID": str(row.reviewerID),
                    "reviewText": str(row.reviewText),
                    "comment_time": int(time.time() * 1000) # Current timestamp in milliseconds
                }

                producer.produce(
                    topic=topic_name,
                    key=record["reviewerID"],
                    value=record,
                    on_delivery=delivery_report
                )
                producer.poll(0)
                
        logging.info("All records sent successfully")
        producer.flush()
        logging.info("Producer flushed successfully")
        return True
    except Exception as e:
        logging.error(f"Error producing records: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()

    if args.mode == "setup":
        # Teardown first if setup is specified
        teardown_topic(
            topic_name=TOPIC_NAME, 
            kafka_bootstrap=KAFKA_BOOTSTRAP
            )
        produce_comment_events(
            avro_schema_path=AVRO_SCHEMA_PATH, 
            schema_registry_url=SCHEMA_REGISTRY_URL, 
            kafka_bootstrap=KAFKA_BOOTSTRAP, 
            review_parquet_path=REVIEW_PARQUET_PATH, 
            topic_name=TOPIC_NAME, 
            num_partitions=5, 
            replication_factor=3
            )
    elif args.mode == "teardown":
        teardown_topic(
            topic_name=TOPIC_NAME, 
            kafka_bootstrap=KAFKA_BOOTSTRAP
            )