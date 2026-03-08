"""Main LLM Insight processor that orchestrates consumption and insight generation"""
import asyncio
import json
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.logging import get_logger
from src.common.config import LLMInsightConfig
from .models import CombinedStats, LLMInsight
from .client import OpenAIClient

logger = get_logger(__name__)


class LLMInsightProcessor:
    """
    Processor that:
    1. Consumes combined stats from ksqlDB output topic
    2. Generates insights using OpenAI
    3. Produces insights to output topic
    """
    
    def __init__(self, config: LLMInsightConfig):
        self.config = config
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self.openai_client: Optional[OpenAIClient] = None
        self._running = False
        self._processed_count = 0
    
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
    
    def _parse_combined_stats(self, msg_value: bytes) -> Optional[CombinedStats]:
        """Parse combined stats message"""
        try:
            data = json.loads(msg_value.decode("utf-8"))
            normalized = {k.lower(): v for k, v in data.items()}
            return CombinedStats(**normalized)
        except Exception as e:
            logger.error(f"Failed to parse combined stats: {e}")
            return None
    
    def _delivery_callback(self, err, msg):
        """Callback for producer delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Insight delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
            )
    
    def _produce_insight(self, insight: LLMInsight):
        """Produce insight to output topic"""
        try:
            key = f"{insight.window_start}-{insight.window_end}"
            self.producer.produce(
                topic=self.config.output_topic,
                key=key,
                value=insight.model_dump_json().encode("utf-8"),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
        except BufferError:
            logger.warning("Producer buffer full, waiting...")
            self.producer.poll(1)
            self.producer.produce(
                topic=self.config.output_topic,
                key=f"{insight.window_start}-{insight.window_end}",
                value=insight.model_dump_json().encode("utf-8"),
                callback=self._delivery_callback
            )
    
    async def _process_stats(self, stats: CombinedStats):
        """Process combined stats and generate insight"""
        logger.info(
            f"Processing window {stats.window_key} "
            f"(comments={stats.total_count})"
        )
        
        summary, recommendations = await self.openai_client.generate_insight(stats)
        
        insight = LLMInsight.from_combined_stats(
            stats=stats,
            summary=summary,
            recommendations=recommendations,
            model=self.config.openai_model,
        )
        
        self._produce_insight(insight)
        self._processed_count += 1
        
        logger.info(
            f"Generated insight for window {stats.window_key}: "
            f"{len(summary)} chars, {len(recommendations)} recommendations"
        )
    
    async def _handle_message(self, msg_value: bytes):
        """Handle incoming message from combined stats topic"""
        stats = self._parse_combined_stats(msg_value)
        if stats is None:
            return
        
        await self._process_stats(stats)
    
    async def run(self):
        """Main processing loop"""
        logger.info("Starting LLM Insight Processor...")
        
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()
        self.openai_client = OpenAIClient(self.config)
        
        self._ensure_output_topic_exists()
        
        self.consumer.subscribe([self.config.combined_stats_topic])
        logger.info(f"Subscribed to topic: {self.config.combined_stats_topic}")
        
        self._running = True
        
        async with self.openai_client:
            try:
                while self._running:
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
                    
                    await self._handle_message(msg.value())
                    
                    if self._processed_count > 0 and self._processed_count % 10 == 0:
                        logger.info(f"Processed {self._processed_count} windows")
                        
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                raise
            finally:
                await self.shutdown()
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down LLM Insight Processor...")
        self._running = False
        
        if self.producer:
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered")
            logger.info("Producer flushed")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")
        
        if self.openai_client:
            stats = self.openai_client.stats
            logger.info(f"OpenAI usage: {stats}")
        
        logger.info(
            f"LLM Insight Processor shutdown complete. "
            f"Total windows processed: {self._processed_count}"
        )
    
    def stop(self):
        """Signal the processor to stop"""
        self._running = False
