"""Kafka admin client for topic management"""
from confluent_kafka.admin import AdminClient, NewTopic
from typing import List, Dict, Any, Optional
from src.common.logging import get_logger
from src.common.config.kafka_config import KafkaConfig

logger = get_logger(__name__)

class KafkaAdminClient:
    """
    Kafka admin client for topic management.
    
    Example:
        admin = KafkaAdminClient("localhost:9092")
        admin.create_topic("my-topic", num_partitions=5, replication_factor=3)
        admin.delete_topic("old-topic")
    """
    
    def __init__(self, bootstrap_servers: str):
        """
        Initialize Kafka admin client.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
        """
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        logger.info(f"Kafka Admin Client initialized for {bootstrap_servers}")
    
    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Create a new Kafka topic.
        
        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions
            replication_factor: Replication factor
            config: Topic configuration
        
        Returns:
            True if successful, False otherwise
        """
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config or {}
        )
        
        futures = self.admin_client.create_topics([new_topic])
        
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {e}")
                return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a Kafka topic.
        
        Args:
            topic_name: Name of the topic to delete
        
        Returns:
            True if successful, False otherwise
        """
        futures = self.admin_client.delete_topics([topic_name])
        
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic '{topic}' deleted successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to delete topic '{topic}': {e}")
                return False
    
    def list_topics(self, timeout: float = 10.0) -> List[str]:
        """
        List all topics in the Kafka cluster.
        
        Args:
            timeout: Timeout in seconds
        
        Returns:
            List of topic names
        """
        metadata = self.admin_client.list_topics(timeout=timeout)
        topics = list(metadata.topics.keys())
        logger.info(f"Found {len(topics)} topics")
        return topics
    
    def topic_exists(self, topic_name: str, timeout: float = 10.0) -> bool:
        """
        Check if a topic exists.
        
        Args:
            topic_name: Name of the topic
            timeout: Timeout in seconds
        
        Returns:
            True if topic exists, False otherwise
        """
        topics = self.list_topics(timeout)
        return topic_name in topics