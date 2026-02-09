# Refactor Plan: Livestream Assistant Agent (Step-by-Step)

## Executive Summary

This plan focuses on **eliminating code duplication** and creating **shared utilities** with **configuration management** as the top priority. The refactor is designed to be **non-breaking** initially, allowing existing code to continue working while new utilities are introduced. Unit tests will be added for all new components.

**User Priorities:**
1. ✅ Code duplication and shared utilities
2. ✅ Configuration management  
3. ✅ Add unit tests
4. ⏸️ Streaming module - leave as placeholder
5. ❌ Breaking changes - avoid for now

---

## Refactor Strategy

### Phase 1: Foundation (Non-Breaking) ← **START HERE**
Create shared utilities without modifying existing code. Existing modules can continue using their current approach.

### Phase 2: Migration (Incremental)
Gradually migrate existing modules to use shared utilities, one module at a time.

### Phase 3: Consolidation (Breaking)
Consolidate schemas and data files (deferred based on user feedback).

---

## Phase 1: Foundation Setup

### Step 1: Directory Structure Setup

Create the shared utilities directory structure:

```bash
mkdir -p src/common/config
mkdir -p src/common/kafka
mkdir -p src/common/storage
mkdir -p src/common/db
mkdir -p src/common/logging
mkdir -p src/schemas/avro
mkdir -p tests/unit/common/config
mkdir -p tests/unit/common/kafka
mkdir -p tests/unit/common/storage
mkdir -p tests/integration
```

**Status**: ✅ [src/common/config/base.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/common/config/base.py) already created by user

---

### Step 2: Configuration Management

#### File: `src/common/config/__init__.py`

```python
"""
Configuration management utilities for all services.

Provides base configuration classes with Pydantic validation.
"""

from .base import BaseConfig
from .kafka_config import KafkaConfig
from .redis_config import RedisConfig
from .postgres_config import PostgresConfig

__all__ = [
    "BaseConfig",
    "KafkaConfig",
    "RedisConfig",
    "PostgresConfig",
]
```

---

#### File: [src/common/config/base.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/common/config/base.py)

✅ **Already created by user** - verify it matches this version:

```python
from pydantic import BaseSettings, Field, validator
from typing import Optional

class BaseConfig(BaseSettings):
    """Base configuration with validation"""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        validate_assignment = True

    @validator("*", pre=True)
    def validate_required(cls, v, field):
        if v is None and field.required:
            raise ValueError(f"{field.name} is required")
        return v
```

---

#### File: `src/common/config/kafka_config.py`

```python
"""Kafka configuration with validation"""
from typing import Optional, Literal
from pydantic import Field, validator
from .base import BaseConfig


class KafkaConfig(BaseConfig):
    """Kafka connection and producer/consumer configuration"""
    
    # Broker configuration
    bootstrap_servers: str = Field(
        ..., 
        description="Kafka bootstrap servers",
        env="KAFKA_BOOTSTRAP_SERVERS"
    )
    
    # Schema Registry
    schema_registry_url: str = Field(
        ...,
        description="Schema Registry URL",
        env="SCHEMA_REGISTRY_URL"
    )
    
    # Topic configuration
    topic_name: Optional[str] = Field(
        None,
        description="Default topic name",
        env="KAFKA_TOPIC_NAME"
    )
    
    num_partitions: int = Field(
        default=5,
        description="Number of partitions for new topics",
        env="KAFKA_NUM_PARTITIONS"
    )
    
    replication_factor: int = Field(
        default=3,
        description="Replication factor for new topics",
        env="KAFKA_REPLICATION_FACTOR"
    )
    
    # Producer configuration
    producer_acks: Literal["all", "0", "1"] = Field(
        default="all",
        description="Producer acknowledgment mode",
        env="KAFKA_PRODUCER_ACKS"
    )
    
    producer_retries: int = Field(
        default=10,
        description="Number of retries for producer",
        env="KAFKA_PRODUCER_RETRIES"
    )
    
    retry_backoff_ms: int = Field(
        default=500,
        description="Retry backoff in milliseconds",
        env="KAFKA_RETRY_BACKOFF_MS"
    )
    
    # Consumer configuration
    consumer_group_id: Optional[str] = Field(
        None,
        description="Consumer group ID",
        env="KAFKA_CONSUMER_GROUP_ID"
    )
    
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        default="earliest",
        description="Auto offset reset policy",
        env="KAFKA_AUTO_OFFSET_RESET"
    )
    
    @validator("bootstrap_servers")
    def validate_bootstrap_servers(cls, v):
        if not v or v.strip() == "":
            raise ValueError("bootstrap_servers cannot be empty")
        return v.strip()
    
    @validator("schema_registry_url")
    def validate_schema_registry_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("schema_registry_url must start with http:// or https://")
        return v.rstrip("/")
    
    @validator("num_partitions")
    def validate_num_partitions(cls, v):
        if v < 1:
            raise ValueError("num_partitions must be at least 1")
        return v
    
    @validator("replication_factor")
    def validate_replication_factor(cls, v):
        if v < 1:
            raise ValueError("replication_factor must be at least 1")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        validate_assignment = True
```

---

#### File: `src/common/config/redis_config.py`

```python
"""Redis configuration with validation"""
from pydantic import Field, validator
from .base import BaseConfig


class RedisConfig(BaseConfig):
    """Redis connection configuration"""
    
    host: str = Field(
        default="localhost",
        description="Redis host",
        env="REDIS_HOST"
    )
    
    port: int = Field(
        default=6379,
        description="Redis port",
        env="REDIS_PORT"
    )
    
    db: int = Field(
        default=0,
        description="Redis database number",
        env="REDIS_DB"
    )
    
    password: str | None = Field(
        default=None,
        description="Redis password",
        env="REDIS_PASSWORD"
    )
    
    socket_timeout: int = Field(
        default=5,
        description="Socket timeout in seconds",
        env="REDIS_SOCKET_TIMEOUT"
    )
    
    socket_connect_timeout: int = Field(
        default=5,
        description="Socket connect timeout in seconds",
        env="REDIS_SOCKET_CONNECT_TIMEOUT"
    )
    
    decode_responses: bool = Field(
        default=True,
        description="Decode responses to strings",
        env="REDIS_DECODE_RESPONSES"
    )
    
    max_connections: int = Field(
        default=50,
        description="Maximum number of connections in pool",
        env="REDIS_MAX_CONNECTIONS"
    )
    
    @validator("port")
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v
    
    @validator("db")
    def validate_db(cls, v):
        if not 0 <= v <= 15:
            raise ValueError("db must be between 0 and 15")
        return v
    
    def get_connection_kwargs(self) -> dict:
        """Get connection parameters for Redis client"""
        kwargs = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "decode_responses": self.decode_responses,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "max_connections": self.max_connections,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        validate_assignment = True
```

---

#### File: `src/common/config/postgres_config.py`

```python
"""PostgreSQL configuration with validation"""
from pydantic import Field, validator
from .base import BaseConfig


class PostgresConfig(BaseConfig):
    """PostgreSQL connection configuration"""
    
    host: str = Field(
        default="localhost",
        description="PostgreSQL host",
        env="POSTGRES_HOST"
    )
    
    port: int = Field(
        default=5432,
        description="PostgreSQL port",
        env="POSTGRES_PORT"
    )
    
    database: str = Field(
        ...,
        description="PostgreSQL database name",
        env="POSTGRES_DB"
    )
    
    user: str = Field(
        ...,
        description="PostgreSQL user",
        env="POSTGRES_USER"
    )
    
    password: str = Field(
        ...,
        description="PostgreSQL password",
        env="POSTGRES_PASSWORD"
    )
    
    # Connection pool settings
    pool_size: int = Field(
        default=10,
        description="Connection pool size",
        env="POSTGRES_POOL_SIZE"
    )
    
    max_overflow: int = Field(
        default=20,
        description="Maximum overflow connections",
        env="POSTGRES_MAX_OVERFLOW"
    )
    
    pool_recycle: int = Field(
        default=3600,
        description="Connection recycle time in seconds",
        env="POSTGRES_POOL_RECYCLE"
    )
    
    pool_pre_ping: bool = Field(
        default=True,
        description="Enable connection health checks",
        env="POSTGRES_POOL_PRE_PING"
    )
    
    echo: bool = Field(
        default=False,
        description="Echo SQL statements",
        env="POSTGRES_ECHO"
    )
    
    @validator("port")
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError("port must be between 1 and 65535")
        return v
    
    @validator("pool_size")
    def validate_pool_size(cls, v):
        if v < 1:
            raise ValueError("pool_size must be at least 1")
        return v
    
    def get_connection_url(self, hide_password: bool = False) -> str:
        """Get PostgreSQL connection URL"""
        password = "***" if hide_password else self.password
        return f"postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.database}"
    
    def get_engine_kwargs(self) -> dict:
        """Get SQLAlchemy engine parameters"""
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "echo": self.echo,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        validate_assignment = True
```

---

### Step 3: Logging Utilities

#### File: `src/common/logging/__init__.py`

```python
"""Centralized logging configuration"""
from .config import setup_logging, get_logger

__all__ = ["setup_logging", "get_logger"]
```

---

#### File: `src/common/logging/config.py`

```python
"""Logging configuration utilities"""
import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
) -> None:
    """
    Setup centralized logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string. If None, uses default.
        include_timestamp: Include timestamp in logs
    """
    if format_string is None:
        if include_timestamp:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        else:
            format_string = "%(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a logger with optional level override.
    
    Args:
        name: Logger name (usually __name__)
        level: Optional level override for this logger
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    if level:
        logger.setLevel(getattr(logging, level.upper()))
    return logger
```

---

### Step 4: Storage Utilities

#### File: `src/common/storage/__init__.py`

```python
"""Storage utilities for parquet, Redis, etc."""
from .parquet_reader import ParquetBatchReader
from .redis_client import RedisClientManager

__all__ = ["ParquetBatchReader", "RedisClientManager"]
```

---

#### File: `src/common/storage/parquet_reader.py`

```python
"""Parquet batch reader utility"""
import pyarrow.parquet as pq
from typing import Iterator, Callable, Optional, Dict, Any
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ParquetBatchReader:
    """
    Batch reader for parquet files with progress tracking and transformation support.
    
    Example:
        reader = ParquetBatchReader("data/comments.parquet", batch_size=1000)
        for batch_df in reader.iter_batches(max_records=10000):
            # Process batch_df
            pass
    """
    
    def __init__(self, file_path: str, batch_size: int = 1000):
        """
        Initialize parquet reader.
        
        Args:
            file_path: Path to parquet file
            batch_size: Number of records per batch
        
        Raises:
            FileNotFoundError: If parquet file doesn't exist
            ValueError: If batch_size < 1
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        
        self.batch_size = batch_size
        self.parquet_file = pq.ParquetFile(str(file_path))
        
        logger.info(f"Initialized ParquetBatchReader for {file_path} with batch_size={batch_size}")
    
    def iter_batches(
        self,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        max_records: Optional[int] = None,
        columns: Optional[list[str]] = None,
        filters: Optional[list] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Iterate over batches with optional transformation and filtering.
        
        Args:
            transform: Optional function to transform each batch DataFrame
            max_records: Maximum total records to process (None = all)
            columns: List of columns to read (None = all)
            filters: PyArrow filters to apply
        
        Yields:
            DataFrame batches
        """
        total_processed = 0
        batch_num = 0
        
        for batch in self.parquet_file.iter_batches(
            batch_size=self.batch_size,
            columns=columns,
        ):
            batch_df = batch.to_pandas()
            batch_num += 1
            
            # Apply transformation if provided
            if transform:
                try:
                    batch_df = transform(batch_df)
                except Exception as e:
                    logger.error(f"Error transforming batch {batch_num}: {e}")
                    raise
            
            yield batch_df
            
            total_processed += len(batch_df)
            logger.debug(f"Processed batch {batch_num}: {len(batch_df)} records (total: {total_processed})")
            
            # Check max_records limit
            if max_records and total_processed >= max_records:
                logger.info(f"Reached max_records limit: {max_records}")
                break
        
        logger.info(f"Completed processing {total_processed} records in {batch_num} batches")
    
    def get_schema(self) -> pq.Schema:
        """Get parquet file schema"""
        return self.parquet_file.schema
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get parquet file metadata"""
        metadata = self.parquet_file.metadata
        return {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "serialized_size": metadata.serialized_size,
        }
    
    def __repr__(self) -> str:
        return f"ParquetBatchReader(file_path='{self.file_path}', batch_size={self.batch_size})"
```

---

#### File: `src/common/storage/redis_client.py`

```python
"""Redis client manager with connection pooling and retry logic"""
import redis.asyncio as redis
from redis.asyncio import Redis
from typing import Optional
import logging
from contextlib import asynccontextmanager
from ..config.redis_config import RedisConfig

logger = logging.getLogger(__name__)


class RedisClientManager:
    """
    Redis client manager with async support and connection pooling.
    
    Example:
        config = RedisConfig()
        client = await RedisClientManager.create(config)
        await client.set("key", "value")
        await client.close()
    """
    
    @staticmethod
    async def create(config: RedisConfig) -> Redis:
        """
        Create and validate Redis client.
        
        Args:
            config: RedisConfig instance
        
        Returns:
            Connected Redis client
        
        Raises:
            redis.ConnectionError: If connection fails
        """
        connection_kwargs = config.get_connection_kwargs()
        
        client = redis.Redis(**connection_kwargs)
        
        # Test connection
        try:
            await client.ping()
            logger.info(f"Successfully connected to Redis at {config.host}:{config.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            await client.close()
            raise
        
        return client
    
    @staticmethod
    @asynccontextmanager
    async def get_client(config: RedisConfig):
        """
        Context manager for Redis client.
        
        Example:
            async with RedisClientManager.get_client(config) as client:
                await client.set("key", "value")
        """
        client = await RedisClientManager.create(config)
        try:
            yield client
        finally:
            await client.close()
            logger.info("Redis connection closed")
```

---

### Step 5: Kafka Utilities

#### File: `src/common/kafka/__init__.py`

```python
"""Kafka utilities for producers, consumers, and admin operations"""
from .producer import KafkaProducerClient
from .admin import KafkaAdminClient

__all__ = ["KafkaProducerClient", "KafkaAdminClient"]
```

---

#### File: `src/common/kafka/producer.py`

```python
"""Kafka producer client with retry logic and error handling"""
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from typing import Callable, Optional, Dict, Any
import logging
from pathlib import Path
import time

logger = logging.getLogger(__name__)


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
```

---

#### File: `src/common/kafka/admin.py`

```python
"""Kafka admin client for topic management"""
from confluent_kafka.admin import AdminClient, NewTopic
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


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
```

---

### Step 6: Database Utilities

#### File: `src/common/db/__init__.py`

```python
"""Database utilities for PostgreSQL"""
from .postgres import PostgresClient

__all__ = ["PostgresClient"]
```

---

#### File: `src/common/db/postgres.py`

```python
"""PostgreSQL client with connection pooling and improved error handling"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Optional
import logging
from contextlib import contextmanager
from ..config.postgres_config import PostgresConfig

logger = logging.getLogger(__name__)


class PostgresClient:
    """
    PostgreSQL client with connection pooling and session management.
    
    Example:
        config = PostgresConfig()
        client = PostgresClient(config)
        
        with client.get_session() as session:
            # Use session
            pass
    """
    
    def __init__(self, config: PostgresConfig):
        """
        Initialize PostgreSQL client.
        
        Args:
            config: PostgresConfig instance
        """
        self.config = config
        self.engine = None
        self.SessionLocal = None
        self._connect()
    
    def _connect(self):
        """Establish database connection with pooling"""
        connection_url = self.config.get_connection_url(hide_password=False)
        
        # Log connection (without password)
        logger.info(f"Connecting to PostgreSQL: {self.config.get_connection_url(hide_password=True)}")
        
        # Create engine with connection pooling
        self.engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            **self.config.get_engine_kwargs()
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        # Test connection
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            logger.info("PostgreSQL connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Get database session with automatic cleanup.
        
        Yields:
            SQLAlchemy Session
        
        Example:
            with client.get_session() as session:
                session.query(Model).all()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def create_tables(self, base):
        """
        Create all tables for a declarative base.
        
        Args:
            base: SQLAlchemy declarative base
        """
        base.metadata.create_all(self.engine)
        logger.info("Database tables created")
    
    def drop_tables(self, base):
        """
        Drop all tables for a declarative base.
        
        Args:
            base: SQLAlchemy declarative base
        """
        base.metadata.drop_all(self.engine)
        logger.info("Database tables dropped")
    
    def close(self):
        """Close all connections and dispose engine"""
        if self.engine:
            self.engine.dispose()
            logger.info("PostgreSQL connection pool disposed")
```

---

### Step 7: Unit Tests

#### File: `tests/conftest.py`

```python
"""Pytest configuration and fixtures"""
import pytest
import os
from pathlib import Path

# Set test environment
os.environ["TESTING"] = "1"

@pytest.fixture
def test_data_dir():
    """Return path to test data directory"""
    return Path(__file__).parent / "data"

@pytest.fixture
def sample_parquet_file(test_data_dir, tmp_path):
    """Create a sample parquet file for testing"""
    import pandas as pd
    import pyarrow.parquet as pq
    
    # Create sample data
    df = pd.DataFrame({
        "comment_id": [1, 2, 3, 4, 5],
        "user_id": ["user1", "user2", "user3", "user4", "user5"],
        "comments": ["Great!", "Nice product", "Amazing", "Love it", "Excellent"],
        "event_timestamp": [1000, 2000, 3000, 4000, 5000]
    })
    
    # Save to parquet
    parquet_file = tmp_path / "test_data.parquet"
    df.to_parquet(parquet_file, index=False)
    
    return str(parquet_file)
```

---

#### File: `tests/unit/common/config/test_kafka_config.py`

```python
"""Unit tests for Kafka configuration"""
import pytest
from pydantic import ValidationError
import os


def test_kafka_config_valid():
    """Test valid Kafka configuration"""
    from src.common.config.kafka_config import KafkaConfig
    
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    
    config = KafkaConfig()
    
    assert config.bootstrap_servers == "localhost:9092"
    assert config.schema_registry_url == "http://localhost:8081"
    assert config.num_partitions == 5
    assert config.replication_factor == 3


def test_kafka_config_custom_values():
    """Test Kafka configuration with custom values"""
    from src.common.config.kafka_config import KafkaConfig
    
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "broker1:9092,broker2:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "https://schema-registry:8081"
    os.environ["KAFKA_NUM_PARTITIONS"] = "10"
    os.environ["KAFKA_REPLICATION_FACTOR"] = "2"
    
    config = KafkaConfig()
    
    assert config.num_partitions == 10
    assert config.replication_factor == 2


def test_kafka_config_validation_errors():
    """Test Kafka configuration validation"""
    from src.common.config.kafka_config import KafkaConfig
    
    # Missing required field
    os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
    with pytest.raises(ValidationError):
        KafkaConfig()
    
    # Invalid schema registry URL
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["SCHEMA_REGISTRY_URL"] = "invalid-url"
    
    with pytest.raises(ValidationError) as exc_info:
        KafkaConfig()
    assert "schema_registry_url" in str(exc_info.value)
```

---

#### File: `tests/unit/common/storage/test_parquet_reader.py`

```python
"""Unit tests for ParquetBatchReader"""
import pytest
import pandas as pd


def test_parquet_reader_initialization(sample_parquet_file):
    """Test ParquetBatchReader initialization"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    assert reader.batch_size == 2
    assert reader.parquet_file is not None


def test_parquet_reader_file_not_found():
    """Test ParquetBatchReader with non-existent file"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    with pytest.raises(FileNotFoundError):
        ParquetBatchReader("non_existent_file.parquet")


def test_parquet_reader_iter_batches(sample_parquet_file):
    """Test iterating over batches"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches())
    
    # Should have 3 batches (5 records with batch_size=2)
    assert len(batches) == 3
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2
    assert len(batches[2]) == 1


def test_parquet_reader_max_records(sample_parquet_file):
    """Test max_records parameter"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches(max_records=3))
    
    # Should stop after 3 records (2 batches)
    assert len(batches) == 2
    total_records = sum(len(batch) for batch in batches)
    assert total_records == 4  # 2 + 2 (stops after reaching 3+)


def test_parquet_reader_transform(sample_parquet_file):
    """Test batch transformation"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    def uppercase_comments(df):
        df["comments"] = df["comments"].str.upper()
        return df
    
    reader = ParquetBatchReader(sample_parquet_file, batch_size=2)
    
    batches = list(reader.iter_batches(transform=uppercase_comments))
    
    # Check that transformation was applied
    for batch in batches:
        assert all(batch["comments"].str.isupper())


def test_parquet_reader_metadata(sample_parquet_file):
    """Test getting parquet metadata"""
    from src.common.storage.parquet_reader import ParquetBatchReader
    
    reader = ParquetBatchReader(sample_parquet_file)
    metadata = reader.get_metadata()
    
    assert metadata["num_rows"] == 5
    assert metadata["num_columns"] == 4
    assert "num_row_groups" in metadata
```

---

#### File: `tests/unit/common/kafka/test_admin.py`

```python
"""Unit tests for Kafka admin client"""
import pytest
from unittest.mock import Mock, patch


def test_kafka_admin_initialization():
    """Test KafkaAdminClient initialization"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin:
        admin = KafkaAdminClient("localhost:9092")
        
        mock_admin.assert_called_once_with({"bootstrap.servers": "localhost:9092"})
        assert admin.bootstrap_servers == "localhost:9092"


def test_kafka_admin_create_topic():
    """Test topic creation"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin_class:
        mock_admin = Mock()
        mock_admin_class.return_value = mock_admin
        
        # Mock successful topic creation
        mock_future = Mock()
        mock_future.result.return_value = None
        mock_admin.create_topics.return_value = {"test-topic": mock_future}
        
        admin = KafkaAdminClient("localhost:9092")
        result = admin.create_topic("test-topic", num_partitions=5, replication_factor=3)
        
        assert result is True
        mock_admin.create_topics.assert_called_once()


def test_kafka_admin_delete_topic():
    """Test topic deletion"""
    from src.common.kafka.admin import KafkaAdminClient
    
    with patch("src.common.kafka.admin.AdminClient") as mock_admin_class:
        mock_admin = Mock()
        mock_admin_class.return_value = mock_admin
        
        # Mock successful topic deletion
        mock_future = Mock()
        mock_future.result.return_value = None
        mock_admin.delete_topics.return_value = {"test-topic": mock_future}
        
        admin = KafkaAdminClient("localhost:9092")
        result = admin.delete_topic("test-topic")
        
        assert result is True
        mock_admin.delete_topics.assert_called_once_with(["test-topic"])
```

---

#### File: [requirements.txt](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/agent/requirements.txt) (root level)

Create a unified requirements file:

```txt
# Configuration
pydantic[dotenv]>=2.0.0
python-dotenv>=1.0.0

# Kafka
confluent-kafka==2.12.2

# Database
sqlalchemy>=2.0.45
psycopg>=3.3.2
psycopg2-binary>=2.9.11
alembic>=1.17.2

# Storage
pandas>=2.3.3
pyarrow>=22.0.0
fastparquet>=2025.12.0
duckdb==1.4.3

# Redis
redis[hiredis]>=5.0.0

# API
fastapi>=0.127.0
uvicorn>=0.40.0
httpx>=0.28.1

# ML/NLP
pyspark==3.5.1
spark-nlp==6.2.3
numpy>=2.2.6

# AWS
boto3>=1.34.0

# Utilities
tqdm>=4.66.0
attrs>=25.4.0
certifi>=2025.11.12
requests>=2.32.5
gdown==5.2.0

# Development tools
ipykernel==7.1.0

# Testing
pytest>=7.4.0
pytest-asyncio>=0.21.0
pytest-cov>=4.1.0
pytest-mock>=3.12.0

# Code quality
ruff>=0.1.0
mypy>=1.7.0
```

---

### Step 8: Add Python Path Configuration

#### File: [src/common/__init__.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/common/__init__.py)

```python
"""Common utilities package"""
__version__ = "0.1.0"
```

---

#### File: `pytest.ini` (root level)

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --strict-markers
    --tb=short
    --cov=src/common
    --cov-report=term-missing
    --cov-report=html

markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
```

---

## Verification Plan

### 1. Install Dependencies

```bash
# Install all dependencies
pip install -r requirements.txt

# Or use uv for faster installation
uv pip install -r requirements.txt
```

### 2. Run Unit Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/common/config/test_kafka_config.py

# Run with coverage
pytest --cov=src/common --cov-report=html

# View coverage report
open htmlcov/index.html
```

### 3. Type Checking

```bash
# Run mypy type checker
mypy src/common --ignore-missing-imports
```

### 4. Code Linting

```bash
# Run ruff linter
ruff check src/common

# Auto-fix issues
ruff check src/common --fix
```

### 5. Manual Testing

#### Test Configuration Loading

```python
# Test script: test_config_manual.py
import os
from src.common.config.kafka_config import KafkaConfig
from src.common.config.redis_config import RedisConfig

# Set environment variables
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"

# Load config
kafka_config = KafkaConfig()
print(f"Kafka Config: {kafka_config.dict()}")

redis_config = RedisConfig()
print(f"Redis Config: {redis_config.dict()}")
```

Run: `python test_config_manual.py`

---

#### Test Parquet Reader

```python
# Test script: test_parquet_manual.py
from src.common.storage.parquet_reader import ParquetBatchReader

# Create sample data
import pandas as pd
df = pd.DataFrame({
    "id": range(100),
    "value": range(100, 200)
})
df.to_parquet("test_data.parquet", index=False)

# Test reader
reader = ParquetBatchReader("test_data.parquet", batch_size=20)

for i, batch in enumerate(reader.iter_batches(max_records=50)):
    print(f"Batch {i}: {len(batch)} records")

print(reader.get_metadata())
```

Run: `python test_parquet_manual.py`

---

## Phase 1 Summary

After completing Phase 1, you will have:

✅ **Shared configuration management** with Pydantic validation  
✅ **Centralized logging** utilities  
✅ **Reusable Kafka utilities** (producer, admin)  
✅ **Storage utilities** (Parquet reader, Redis client)  
✅ **Database utilities** (PostgreSQL client with pooling)  
✅ **Comprehensive unit tests** for all utilities  
✅ **Code quality tools** configured (pytest, ruff, mypy)

**Next Steps**: Phase 2 will migrate existing modules to use these shared utilities.

---

## Phase 2: Migration (Deferred)

Phase 2 will involve:
1. Migrating `agent/` module to use shared utilities
2. Migrating `producer/` module to use shared utilities
3. Migrating `cdc_producer/` module to use shared utilities

This will be detailed after Phase 1 is complete and approved.
