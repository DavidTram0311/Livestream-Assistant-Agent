"""LLM Insight service configuration"""
from pydantic import Field, field_validator
from .base import BaseConfig


class LLMInsightConfig(BaseConfig):
    """Configuration for the LLM Insight extraction service"""
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    
    kafka_consumer_group_id: str = Field(
        default="llm-insight-group",
        description="Kafka consumer group ID",
        validation_alias="LLM_CONSUMER_GROUP_ID"
    )
    
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        description="Auto offset reset policy",
        validation_alias="KAFKA_AUTO_OFFSET_RESET"
    )
    
    # Input Topic (from ksqlDB aggregation)
    combined_stats_topic: str = Field(
        default="streaming.combined_stats",
        description="Topic for combined gender and sentiment statistics from ksqlDB",
        validation_alias="LLM_COMBINED_STATS_TOPIC"
    )
    
    # Output Topic
    output_topic: str = Field(
        default="streaming.llm_insights",
        description="Output topic for LLM-generated insights",
        validation_alias="LLM_OUTPUT_TOPIC"
    )
    
    # OpenAI Configuration
    openai_api_key: str = Field(
        default="",
        description="OpenAI API key",
        validation_alias="OPENAI_API_KEY"
    )
    
    openai_model: str = Field(
        default="gpt-4o",
        description="OpenAI model to use for insight generation",
        validation_alias="OPENAI_MODEL"
    )
    
    openai_max_tokens: int = Field(
        default=500,
        description="Maximum tokens for LLM response",
        validation_alias="OPENAI_MAX_TOKENS"
    )
    
    openai_temperature: float = Field(
        default=0.7,
        description="Temperature for LLM response",
        validation_alias="OPENAI_TEMPERATURE"
    )
    
    # Processing Configuration
    poll_timeout_seconds: float = Field(
        default=1.0,
        description="Kafka poll timeout in seconds",
        validation_alias="POLL_TIMEOUT_SECONDS"
    )
    
    # Retry Configuration
    max_retries: int = Field(
        default=3,
        description="Maximum number of API call retries",
        validation_alias="LLM_MAX_RETRIES"
    )
    
    retry_delay_seconds: float = Field(
        default=1.0,
        description="Initial delay between retries (exponential backoff)",
        validation_alias="LLM_RETRY_DELAY_SECONDS"
    )
    
    @field_validator("openai_api_key", mode="before")
    @classmethod
    def validate_api_key(cls, v):
        """Warn if API key is not set"""
        if not v:
            import warnings
            warnings.warn("OPENAI_API_KEY is not set. LLM insights will not be generated.")
        return v or ""
