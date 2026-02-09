"""Producer module configuration"""
from pydantic import Field
from common.config import KafkaConfig


class ProducerConfig(KafkaConfig):
    """Producer-specific configuration extending KafkaConfig"""
    
    # Kafka bootstrap servers (support both local and container modes)
    kafka_bootstrap_local: str = Field(
        ...,
        description="Kafka bootstrap servers for local mode",
        validation_alias="KAFKA_BOOTSTRAP_LOCAL"
    )
    
    kafka_bootstrap_containers: str = Field(
        ...,
        description="Kafka bootstrap servers for container mode",
        validation_alias="KAFKA_BOOTSTRAP_CONTAINERS"
    )
    
    # Schema and data paths
    avro_schema_path: str = Field(
        default="avro_schemas/comment_events.avsc",
        description="Path to Avro schema file (relative to producer dir)",
        validation_alias="AVRO_SCHEMA_PATH"
    )
    
    review_parquet_path: str = Field(
        default="data/user_comments.parquet",
        description="Path to review parquet file (relative to producer dir)",
        validation_alias="REVIEW_PARQUET_PATH"
    )
    
    # Topic configuration
    output_topic: str = Field(
        ...,
        description="Output Kafka topic name",
        validation_alias="KAFKA_OUTPUT_TOPICS"
    )
    
    # Batch processing
    batch_size: int = Field(
        default=1000,
        description="Batch size for parquet reading",
        validation_alias="BATCH_SIZE"
    )
    
    def get_bootstrap_servers(self, mode: str = "local") -> str:
        """Get bootstrap servers based on mode"""
        if mode == "local":
            return self.kafka_bootstrap_local
        elif mode == "containers":
            return self.kafka_bootstrap_containers
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'local' or 'containers'")
    
    def get_absolute_schema_path(self, base_dir: str) -> str:
        """Get absolute path to Avro schema"""
        import os
        return os.path.join(base_dir, self.avro_schema_path)
    
    def get_absolute_parquet_path(self, base_dir: str) -> str:
        """Get absolute path to parquet file"""
        import os
        return os.path.join(base_dir, self.review_parquet_path)
