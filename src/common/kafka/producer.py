"""Kafka producer client with retry logic and error handling"""
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from typing import Callable, Optional, Dict, Any
from pathlib import Path
import time
from src.common.logging import get_logger

logger = get_logger(__name__)

class KafkaProducerClient:
    """
    Reusable Kafka producer with Avro serialization and error handling.
    
    Example:
        config = KafkaConfig()
        producer = KafkaProducerClient(config, "schemas/comment_events.avsc")
        producer.produce("my-topic", key="user123", value={"comment_id": 1, ...})
        producer.flush()
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        avro_schema_path: Optional[str] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            avro_schema_path: Path to Avro schema file (optional)
            producer_config: Additional producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.avro_schema_path = avro_schema_path
        self.producer = None
        self.avro_serializer = None
        
        # Initialize Schema Registry Client
        self.schema_registry_client = self._init_schema_registry_client()
        
        # Initialize Avro Serializer if schema provided
        if avro_schema_path:
            self.avro_serializer = self._load_avro_serializer(avro_schema_path)
        
        # Initialize Producer
        self.producer = self._init_producer(producer_config or {})
    
    def _init_schema_registry_client(self, max_retries: int = 20, retry_delay: int = 2) -> SchemaRegistryClient:
        """Initialize Schema Registry Client with retry logic"""
        for attempt in range(1, max_retries + 1):
            try:
                client = SchemaRegistryClient({"url": self.schema_registry_url})
                # Test connection
                client.get_subjects()
                logger.info("Schema Registry Client initialized successfully")
                return client
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to initialize Schema Registry Client after {max_retries} attempts")
    
    def _load_avro_serializer(self, schema_path: str) -> AvroSerializer:
        """Load Avro serializer from schema file"""
        schema_file = Path(schema_path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Avro schema file not found: {schema_path}")
        
        with open(schema_file, "r") as f:
            schema_str = f.read()
        
        return AvroSerializer(self.schema_registry_client, schema_str)
    
    def _init_producer(self, custom_config: Dict[str, Any], max_retries: int = 10) -> SerializingProducer:
        """Initialize Kafka producer with retry logic"""
        base_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 500,
        }
        
        # Add Avro serializer if available
        if self.avro_serializer:
            base_config["value.serializer"] = self.avro_serializer
        
        # Merge with custom config
        producer_conf = {**base_config, **custom_config}
        
        for attempt in range(1, max_retries + 1):
            try:
                producer = SerializingProducer(producer_conf)
                logger.info("Kafka Producer initialized successfully")
                return producer
            except Exception as e:
                logger.warning(f"Producer init attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(1)
                else:
                    raise Exception(f"Failed to initialize Producer after {max_retries} attempts")
    
    def produce(
        self,
        topic: str,
        key: str,
        value: Any,
        on_delivery: Optional[Callable] = None,
        max_retries: int = 10,
    ) -> None:
        """
        Produce message with backpressure handling.
        
        Args:
            topic: Kafka topic
            key: Message key
            value: Message value (dict for Avro serialization)
            on_delivery: Delivery callback function
            max_retries: Max retries for BufferError
        """
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery or self._default_delivery_report
                )
                # Poll to handle callbacks
                self.producer.poll(0)
                return
            except BufferError:
                logger.warning(f"Producer queue full (retry {retry_count + 1}/{max_retries}), waiting...")
                self.producer.poll(1)
                retry_count += 1
        
        raise Exception(f"Failed to produce message after {max_retries} retries due to buffer overflow")
    
    def _default_delivery_report(self, err, msg):
        """Default delivery report callback"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )
    
    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush pending messages.
        
        Args:
            timeout: Timeout in seconds
        
        Returns:
            Number of messages still in queue
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered within timeout")
        else:
            logger.info("All messages flushed successfully")
        return remaining
    
    def close(self):
        """Close producer and cleanup"""
        if self.producer:
            self.flush()
        logger.info("Producer closed")