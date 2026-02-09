"""Agent module configuration"""
from pydantic import Field
from common.config import RedisConfig


class AgentConfig(RedisConfig):
    """Agent-specific configuration"""
    
    # Spark NLP settings
    is_apple_silicon: bool = Field(
        default=False,
        description="Whether running on Apple Silicon",
        validation_alias="IS_APPLE_SILICON"
    )
    
    # Sentiment model settings
    sentiment_model_name: str = Field(
        default="sentimentdl_use_twitter",
        description="Sentiment model name",
        validation_alias="SENTIMENT_MODEL_NAME"
    )
    
    encoder_name: str = Field(
        default="tfhub_use",
        description="Encoder model name",
        validation_alias="ENCODER_NAME"
    )
    
    input_col: str = Field(
        default="comments",
        description="Input column name for sentiment analysis",
        validation_alias="INPUT_COL"
    )
    
    use_gpu: bool = Field(
        default=False,
        description="Whether to use GPU for inference",
        validation_alias="USE_GPU"
    )
    
    # API settings
    api_host: str = Field(
        default="0.0.0.0",
        description="API host",
        validation_alias="API_HOST"
    )
    
    api_port: int = Field(
        default=8000,
        description="API port",
        validation_alias="API_PORT"
    )
    
    reload: bool = Field(
        default=False,
        description="Enable auto-reload",
        validation_alias="API_RELOAD"
    )
    
    workers: int = Field(
        default=1,
        description="Number of worker processes",
        validation_alias="API_WORKERS"
    )