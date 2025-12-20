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

load_dotenv()

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
TOPIC_NAME = os.getenv("TOPIC_NAME")
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

def create_topic(topic_name, num_partitions=1, replication_factor=3, kafka_bootstrap=KAFKA_BOOTSTRAP):
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

def teardown_topic(topic_name, kafka_bootstrap=KAFKA_BOOTSTRAP):
    admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap})
    fs = admin_client.delete_topics([topic_name])

    for topic, f in fs.items():
        try:
            f.result()
            logging.info(f"Topic {topic} deleted successfully")
        except Exception as e:
            logging.error(f"Error deleting topic {topic}: {e}")

def produce_comment_events(
    avro_schema_path, 
    schema_registry_url=SCHEMA_REGISTRY_URL, 
    kafka_bootstrap=KAFKA_BOOTSTRAP,
    review_parquet_path: str = REVIEW_PARQUET_PATH
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
            print("Producer initialized successfully")
            break
        except Exception as e:
            logging.error(f"Error initializing Producer: {e}")
            time.sleep(1)
    else:
        raise Exception("Failed to initialize Producer")

    # Load data and prepare for produce
    try:
        df = pd.read_parquet(review_parquet_path)
        print(f"Loaded {len(df)} reviews from {review_parquet_path}")

        # Pre-format all records in parallel
        def format_record(row):
            return {
                "reviewerID": str(row.reviewerID),
                "reviewText": str(row.reviewText),
                "comment_time": int(time.time() * 1000) # Current timestamp in milliseconds
            }
    except Exception as e:
        logging.error(f"Error loading reviews from {review_parquet_path}: {e}")






