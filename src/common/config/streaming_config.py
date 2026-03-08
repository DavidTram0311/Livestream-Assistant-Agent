"""Streaming service configuration"""
import os
from pydantic import Field, field_validator
from .base import BaseConfig


class StreamingConfig(BaseConfig):
    """Configuration for the streaming enrichment service"""
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    
    kafka_consumer_group_id: str = Field(
        default="streaming-enrichment-group",
        description="Kafka consumer group ID",
        validation_alias="KAFKA_CONSUMER_GROUP_ID"
    )
    
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        description="Auto offset reset policy",
        validation_alias="KAFKA_AUTO_OFFSET_RESET"
    )
    
    # Topic Configuration
    input_topic: str = Field(
        default="tracking_postgres_cdc.public.comment_events",
        description="Input CDC topic to consume from",
        validation_alias="INPUT_TOPIC"
    )
    
    output_topic: str = Field(
        default="streaming.enriched_events",
        description="Output topic for enriched events",
        validation_alias="OUTPUT_TOPIC"
    )
    
    # API Configuration
    # Supports both API_BASE_URL and STREAMING_API_BASE_URL env vars
    api_base_url: str = Field(
        default="http://localhost:8000",
        description="Base URL for the API service (gender/sentiment endpoints)",
        validation_alias="API_BASE_URL"
    )
    
    @field_validator("api_base_url", mode="before")
    @classmethod
    def resolve_api_base_url(cls, v):
        """Check STREAMING_API_BASE_URL first, then fall back to API_BASE_URL"""
        streaming_url = os.getenv("STREAMING_API_BASE_URL")
        if streaming_url:
            return streaming_url
        return v if v else "http://localhost:8000"
    
    api_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP request timeout in seconds",
        validation_alias="API_TIMEOUT_SECONDS"
    )
    
    api_max_retries: int = Field(
        default=3,
        description="Maximum number of API call retries",
        validation_alias="API_MAX_RETRIES"
    )
    
    # Processing Configuration
    batch_size: int = Field(
        default=100,
        description="Number of messages to process in a batch",
        validation_alias="BATCH_SIZE"
    )
    
    poll_timeout_seconds: float = Field(
        default=1.0,
        description="Kafka poll timeout in seconds",
        validation_alias="POLL_TIMEOUT_SECONDS"
    )
    
    # Schema Registry (optional, for Avro)
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        description="Schema Registry URL",
        validation_alias="SCHEMA_REGISTRY_URL"
    )
