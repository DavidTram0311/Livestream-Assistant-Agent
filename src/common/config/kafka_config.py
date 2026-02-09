"""Kafka configuration with validation"""
from typing import Optional, Literal
from pydantic import Field, field_validator
from .base import BaseConfig

class KafkaConfig(BaseConfig):
    """Kafka connection and producer/consumer configuration"""
    
    # Broker configuration
    bootstrap_servers: str = Field(
        default="localhost:9092", 
        description="Kafka bootstrap servers",
        validation_alias="BOOTSTRAP_SERVERS"
    )
    
    # Schema Registry
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        description="Schema Registry URL",
        validation_alias="SCHEMA_REGISTRY_URL"
    )
    
    # Topic configuration
    topic_name: Optional[str] = Field(
        None,
        description="Default topic name",
        validation_alias="TOPIC_NAME"
    )
    
    num_partitions: int = Field(
        default=5,
        description="Number of partitions for new topics",
        validation_alias="NUM_PARTITIONS"
    )
    
    replication_factor: int = Field(
        default=3,
        description="Replication factor for new topics",
        validation_alias="REPLICATION_FACTOR"
    )
    
    # Producer configuration
    producer_acks: Literal["all", "0", "1"] = Field(
        default="all",
        description="Producer acknowledgment mode",
        validation_alias="PRODUCER_ACKS"
    )
    
    producer_retries: int = Field(
        default=10,
        description="Number of retries for producer",
        validation_alias="PRODUCER_RETRIES"
    )
    
    retry_backoff_ms: int = Field(
        default=500,
        description="Retry backoff in milliseconds",
        validation_alias="RETRY_BACKOFF_MS"
    )
    
    # Consumer configuration
    consumer_group_id: Optional[str] = Field(
        None,
        description="Consumer group ID",
        validation_alias="CONSUMER_GROUP_ID"
    )
    
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        default="earliest",
        description="Auto offset reset policy",
        validation_alias="AUTO_OFFSET_RESET"
    )
    
    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, v):
        if not v or v.strip() == "":
            raise ValueError("bootstrap_servers cannot be empty")
        return v.strip()
    
    @field_validator("schema_registry_url")
    @classmethod
    def validate_schema_registry_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("schema_registry_url must start with http:// or https://")
        return v.rstrip("/")
    
    @field_validator("num_partitions")
    @classmethod
    def validate_num_partitions(cls, v):
        if v < 1:
            raise ValueError("num_partitions must be at least 1")
        return v
    
    @field_validator("replication_factor")
    @classmethod
    def validate_replication_factor(cls, v):
        if v < 1:
            raise ValueError("replication_factor must be at least 1")
        return v