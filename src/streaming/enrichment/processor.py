"""Main enrichment processor that orchestrates consumption, enrichment, and production"""
import asyncio
import io
import json
import signal
import struct
from typing import Optional, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import httpx
import fastavro
from src.common.logging import get_logger
from src.streaming.config import StreamingConfig
from src.streaming.models import CDCEvent, EnrichedEvent, DebeziumEnvelope
from src.streaming.enrichment.client import EnrichmentClient

logger = get_logger(__name__)


class EnrichmentProcessor:
    """
    Main processor that:
    1. Consumes CDC events from Kafka
    2. Enriches them with gender and sentiment via HTTP APIs
    3. Produces enriched events to output topic
    """
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.enrichment_client: Optional[EnrichmentClient] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._schema_cache: Dict[int, Any] = {}
        self._http_client: Optional[httpx.Client] = None
    
    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer"""
        conf = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "group.id": self.config.kafka_consumer_group_id,
            "auto.offset.reset": self.config.kafka_auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }
        return Consumer(conf)
    
    def _create_producer(self) -> Producer:
        """Create and configure Kafka producer"""
        conf = {
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 500,
        }
        return Producer(conf)
    
    def _ensure_output_topic_exists(self):
        """Ensure the output topic exists, create if not"""
        admin_client = AdminClient({
            "bootstrap.servers": self.config.kafka_bootstrap_servers
        })
        
        try:
            metadata = admin_client.list_topics(timeout=10)
            if self.config.output_topic not in metadata.topics:
                logger.info(f"Creating output topic: {self.config.output_topic}")
                new_topic = NewTopic(
                    self.config.output_topic,
                    num_partitions=3,
                    replication_factor=3
                )
                futures = admin_client.create_topics([new_topic])
                for topic, future in futures.items():
                    try:
                        future.result()
                        logger.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        logger.warning(f"Topic {topic} may already exist: {e}")
            else:
                logger.info(f"Output topic {self.config.output_topic} already exists")
        except Exception as e:
            logger.warning(f"Could not verify/create output topic: {e}")
    
    def _get_schema(self, schema_id: int) -> Any:
        """Fetch and cache Avro schema from Schema Registry"""
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=10.0)
        
        url = f"{self.config.schema_registry_url}/schemas/ids/{schema_id}"
        try:
            response = self._http_client.get(url)
            response.raise_for_status()
            schema_json = response.json()["schema"]
            schema = fastavro.parse_schema(json.loads(schema_json))
            self._schema_cache[schema_id] = schema
            logger.info(f"Cached schema ID {schema_id}")
            return schema
        except Exception as e:
            logger.error(f"Failed to fetch schema {schema_id}: {e}")
            raise
    
    def _decode_avro_message(self, msg_value: bytes) -> Optional[Dict[str, Any]]:
        """
        Decode Avro message with Confluent Schema Registry wire format.
        Wire format: [magic byte (0x00)] [4-byte schema ID] [Avro payload]
        """
        if len(msg_value) < 5:
            logger.error(f"Message too short for Avro wire format: {len(msg_value)} bytes")
            return None
        
        magic_byte = msg_value[0]
        if magic_byte != 0:
            logger.debug("Message does not have Avro magic byte, trying JSON")
            return None
        
        schema_id = struct.unpack(">I", msg_value[1:5])[0]
        avro_payload = msg_value[5:]
        
        try:
            schema = self._get_schema(schema_id)
            reader = io.BytesIO(avro_payload)
            record = fastavro.schemaless_reader(reader, schema)
            return record
        except Exception as e:
            logger.error(f"Failed to decode Avro message (schema_id={schema_id}): {e}")
            return None
    
    def _parse_message(self, msg_value: bytes) -> Optional[CDCEvent]:
        """
        Parse Kafka message value into CDCEvent.
        Handles Avro (Schema Registry wire format), raw JSON, and Debezium envelope formats.
        """
        data = None
        
        # Try Avro decoding first (check for magic byte 0x00)
        if len(msg_value) >= 5 and msg_value[0] == 0:
            data = self._decode_avro_message(msg_value)
            if data is not None:
                logger.debug(f"Successfully decoded Avro message: {list(data.keys())}")
        
        # Fall back to JSON if Avro decoding didn't work
        if data is None:
            try:
                data = json.loads(msg_value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to decode message as JSON: {e}")
                return None
        
        try:
            # Check if this is a Debezium envelope (has 'after' field)
            if "after" in data:
                envelope = DebeziumEnvelope(**data)
                return envelope.extract_event()
            
            # Otherwise, assume it's a direct event format
            return CDCEvent(**data)
            
        except Exception as e:
            logger.error(f"Failed to parse message into CDCEvent: {e}")
            logger.debug(f"Message data: {data}")
            return None
    
    def _delivery_callback(self, err, msg):
        """Callback for producer delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
            )
    
    async def _process_message(self, cdc_event: CDCEvent) -> Optional[EnrichedEvent]:
        """Process a single CDC event and return enriched event"""
        try:
            gender, sentiment = await self.enrichment_client.enrich(
                user_id=cdc_event.user_id,
                text=cdc_event.comments,
                comment_id=cdc_event.comment_id
            )
            
            enriched = EnrichedEvent.from_cdc_event(
                cdc_event=cdc_event,
                gender=gender,
                sentiment=sentiment
            )
            
            logger.debug(
                f"Enriched event {cdc_event.comment_id}: gender={gender}, sentiment={sentiment}"
            )
            
            return enriched
            
        except Exception as e:
            logger.error(f"Error enriching event {cdc_event.comment_id}: {e}")
            return EnrichedEvent.from_cdc_event(cdc_event)
    
    def _produce_enriched_event(self, enriched: EnrichedEvent):
        """Produce enriched event to output topic"""
        try:
            self.producer.produce(
                topic=self.config.output_topic,
                key=str(enriched.comment_id),
                value=enriched.model_dump_json().encode("utf-8"),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
        except BufferError:
            logger.warning("Producer buffer full, waiting...")
            self.producer.poll(1)
            self.producer.produce(
                topic=self.config.output_topic,
                key=str(enriched.comment_id),
                value=enriched.model_dump_json().encode("utf-8"),
                callback=self._delivery_callback
            )
    
    async def run(self):
        """Main processing loop"""
        logger.info("Starting enrichment processor...")
        
        # Initialize components
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()
        self.enrichment_client = EnrichmentClient(self.config)
        
        # Ensure output topic exists
        self._ensure_output_topic_exists()
        
        # Subscribe to input topic
        self.consumer.subscribe([self.config.input_topic])
        logger.info(f"Subscribed to topic: {self.config.input_topic}")
        
        self._running = True
        processed_count = 0
        
        async with self.enrichment_client:
            try:
                while self._running:
                    # Poll for messages
                    msg = self.consumer.poll(timeout=self.config.poll_timeout_seconds)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(
                                f"Reached end of partition {msg.partition()} at offset {msg.offset()}"
                            )
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Parse message
                    cdc_event = self._parse_message(msg.value())
                    if cdc_event is None:
                        continue
                    
                    # Enrich and produce
                    enriched = await self._process_message(cdc_event)
                    if enriched:
                        self._produce_enriched_event(enriched)
                        processed_count += 1
                        
                        if processed_count % 100 == 0:
                            logger.info(f"Processed {processed_count} events")
                    
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                raise
            finally:
                await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down enrichment processor...")
        self._running = False
        
        if self.producer:
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered")
            logger.info("Producer flushed")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")
        
        if self._http_client:
            self._http_client.close()
            logger.info("HTTP client closed")
        
        logger.info("Enrichment processor shutdown complete")
    
    def stop(self):
        """Signal the processor to stop"""
        self._running = False
        self._shutdown_event.set()
