# Phase 2 Implementation Plan: Module Migration

## Executive Summary

Phase 2 focuses on **migrating existing modules** to use the shared utilities created in Phase 1. This migration is designed to be **incremental and non-breaking**, allowing each module to be migrated independently with full backward compatibility.

**Phase 1 Recap:**
- ✅ Shared configuration management (`src/common/config/`)
- ✅ Centralized logging (`src/common/logging/`)
- ✅ Kafka utilities (`src/common/kafka/`)
- ✅ Storage utilities (`src/common/storage/`)
- ✅ Database utilities (`src/common/db/`)
- ✅ Comprehensive unit tests

**Phase 2 Goals:**
1. Migrate `producer/` module to use shared utilities
2. Migrate `cdc_producer/` module to use shared utilities
3. Migrate `agent/` module to use shared utilities
4. Maintain backward compatibility during migration
5. Add integration tests for migrated modules

---

## Migration Strategy

### Principles

1. **One Module at a Time**: Migrate modules independently to minimize risk
2. **Non-Breaking**: Existing functionality must continue to work
3. **Test-Driven**: Add tests before and after migration
4. **Incremental**: Start with simple utilities, then move to complex ones
5. **Backward Compatible**: Keep old code paths until migration is verified

### Migration Order

```
Phase 2.1: Producer Module (Simplest)
    ↓
Phase 2.2: CDC Producer Module (Medium complexity)
    ↓
Phase 2.3: Agent Module (Most complex - FastAPI integration)
```

---

## Phase 2.1: Migrate Producer Module

### Current State Analysis

**File**: [src/producer/produce.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/producer/produce.py)

**Code Duplication Identified:**
- ✅ Manual Kafka producer initialization (lines 75-114)
- ✅ Manual Schema Registry client setup (lines 75-89)
- ✅ Manual topic creation/deletion (lines 41-64)
- ✅ Manual parquet batch reading (lines 128-165)
- ✅ Manual logging configuration (line 168)
- ✅ Manual environment variable loading (lines 16-21)

**Shared Utilities to Use:**
- `KafkaConfig` - Configuration management
- `KafkaProducerClient` - Producer with retry logic
- `KafkaAdminClient` - Topic management
- `ParquetBatchReader` - Batch reading
- `setup_logging`, `get_logger` - Centralized logging

---

### Step 1: Create Producer-Specific Configuration

#### File: `src/producer/config.py` (NEW)

```python
"""Producer module configuration"""
from pydantic import Field
from common.config import KafkaConfig


class ProducerConfig(KafkaConfig):
    """Producer-specific configuration extending KafkaConfig"""
    
    # Kafka bootstrap servers (support both local and container modes)
    kafka_bootstrap_local: str = Field(
        ...,
        description="Kafka bootstrap servers for local mode",
        env="KAFKA_BOOTSTRAP_LOCAL"
    )
    
    kafka_bootstrap_containers: str = Field(
        ...,
        description="Kafka bootstrap servers for container mode",
        env="KAFKA_BOOTSTRAP_CONTAINERS"
    )
    
    # Schema and data paths
    avro_schema_path: str = Field(
        default="avro_schemas/comment_events.avsc",
        description="Path to Avro schema file (relative to producer dir)",
        env="AVRO_SCHEMA_PATH"
    )
    
    review_parquet_path: str = Field(
        default="data/user_comments.parquet",
        description="Path to review parquet file (relative to producer dir)",
        env="REVIEW_PARQUET_PATH"
    )
    
    # Topic configuration
    output_topic: str = Field(
        ...,
        description="Output Kafka topic name",
        env="KAFKA_OUTPUT_TOPICS"
    )
    
    # Batch processing
    batch_size: int = Field(
        default=1000,
        description="Batch size for parquet reading",
        env="BATCH_SIZE"
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
```

---

### Step 2: Refactor Producer Module

#### File: `src/producer/produce_refactored.py` (NEW)

```python
"""Refactored producer using shared utilities"""
import time
import argparse
from pathlib import Path
from common.logging import setup_logging, get_logger
from common.kafka import KafkaProducerClient, KafkaAdminClient
from common.storage import ParquetBatchReader
from .config import ProducerConfig

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with comment events."
)
parser.add_argument(
    "-t",
    "--type",
    default="local",
    choices=["local", "containers"],
    help="The type of Kafka bootstrap servers to use."
)


def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        logger.error(f"Error producing record {msg.key()}: {err}")
    else:
        logger.info(
            f"Record {msg.key()} successfully produced to "
            f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def produce_comment_events(config: ProducerConfig, kafka_mode: str):
    """
    Produce comment events to Kafka using shared utilities.
    
    Args:
        config: ProducerConfig instance
        kafka_mode: 'local' or 'containers'
    """
    base_dir = Path(__file__).parent
    bootstrap_servers = config.get_bootstrap_servers(kafka_mode)
    schema_path = config.get_absolute_schema_path(str(base_dir))
    parquet_path = config.get_absolute_parquet_path(str(base_dir))
    
    logger.info(f"Using Kafka bootstrap servers: {bootstrap_servers}")
    logger.info(f"Schema path: {schema_path}")
    logger.info(f"Parquet path: {parquet_path}")
    
    # Initialize Kafka Admin Client
    admin = KafkaAdminClient(bootstrap_servers)
    
    # Create topic
    logger.info(f"Creating topic: {config.output_topic}")
    admin.create_topic(
        topic_name=config.output_topic,
        num_partitions=config.num_partitions,
        replication_factor=config.replication_factor
    )
    
    # Initialize Kafka Producer
    logger.info("Initializing Kafka producer...")
    producer = KafkaProducerClient(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=config.schema_registry_url,
        avro_schema_path=schema_path
    )
    
    # Initialize Parquet Reader
    logger.info(f"Reading parquet file: {parquet_path}")
    reader = ParquetBatchReader(parquet_path, batch_size=config.batch_size)
    
    # Produce records
    try:
        logger.info("Starting to send records...")
        comment_id = 0
        
        for batch_df in reader.iter_batches():
            for _, row in batch_df.iterrows():
                comment_id += 1
                record = {
                    "comment_id": int(comment_id),
                    "user_id": str(row.reviewerID),
                    "comments": str(row.reviewText),
                    "event_timestamp": int(time.time() * 1000)
                }
                
                producer.produce(
                    topic=config.output_topic,
                    key=record["user_id"],
                    value=record,
                    on_delivery=delivery_report
                )
        
        logger.info("All records sent successfully")
        producer.flush()
        logger.info("Producer flushed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error producing records: {e}")
        return False


def teardown_topic(config: ProducerConfig, kafka_mode: str):
    """
    Delete Kafka topic.
    
    Args:
        config: ProducerConfig instance
        kafka_mode: 'local' or 'containers'
    """
    bootstrap_servers = config.get_bootstrap_servers(kafka_mode)
    admin = KafkaAdminClient(bootstrap_servers)
    
    logger.info(f"Deleting topic: {config.output_topic}")
    admin.delete_topic(config.output_topic)


def main():
    """Main entry point"""
    args = parser.parse_args()
    
    # Load configuration
    config = ProducerConfig()
    
    if args.mode == "setup":
        # Teardown first if setup is specified
        teardown_topic(config, args.type)
        time.sleep(5)
        produce_comment_events(config, args.type)
        
    elif args.mode == "teardown":
        teardown_topic(config, args.type)


if __name__ == "__main__":
    main()
```

---

### Step 3: Update Producer Requirements

#### File: `src/producer/requirements.txt`

```txt
# Remove duplicates, rely on root requirements.txt
# Keep only producer-specific dependencies if any
```

**Action**: Update root `requirements.txt` to include all dependencies from Phase 1.

---

### Step 4: Create Migration Test

#### File: `tests/integration/test_producer_migration.py` (NEW)

```python
"""Integration test for producer migration"""
import pytest
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from src.producer.produce_refactored import produce_comment_events, teardown_topic
from src.producer.config import ProducerConfig


@pytest.fixture
def producer_config():
    """Create test producer configuration"""
    os.environ["KAFKA_BOOTSTRAP_LOCAL"] = "localhost:9092"
    os.environ["KAFKA_BOOTSTRAP_CONTAINERS"] = "broker:29092"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["KAFKA_OUTPUT_TOPICS"] = "test-comments"
    
    return ProducerConfig()


def test_producer_config_loading(producer_config):
    """Test that producer config loads correctly"""
    assert producer_config.kafka_bootstrap_local == "localhost:9092"
    assert producer_config.output_topic == "test-comments"
    assert producer_config.batch_size == 1000


def test_get_bootstrap_servers(producer_config):
    """Test bootstrap server selection"""
    assert producer_config.get_bootstrap_servers("local") == "localhost:9092"
    assert producer_config.get_bootstrap_servers("containers") == "broker:29092"
    
    with pytest.raises(ValueError):
        producer_config.get_bootstrap_servers("invalid")


@patch("src.producer.produce_refactored.KafkaAdminClient")
@patch("src.producer.produce_refactored.KafkaProducerClient")
@patch("src.producer.produce_refactored.ParquetBatchReader")
def test_produce_comment_events_integration(
    mock_reader_class,
    mock_producer_class,
    mock_admin_class,
    producer_config,
    tmp_path
):
    """Test produce_comment_events with mocked dependencies"""
    # Mock admin client
    mock_admin = Mock()
    mock_admin_class.return_value = mock_admin
    
    # Mock producer
    mock_producer = Mock()
    mock_producer_class.return_value = mock_producer
    
    # Mock parquet reader
    import pandas as pd
    mock_reader = Mock()
    mock_df = pd.DataFrame({
        "reviewerID": ["user1", "user2"],
        "reviewText": ["Great!", "Nice!"]
    })
    mock_reader.iter_batches.return_value = [mock_df]
    mock_reader_class.return_value = mock_reader
    
    # Create dummy schema file
    schema_path = tmp_path / "avro_schemas" / "comment_events.avsc"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text('{"type": "record", "name": "Comment"}')
    
    # Create dummy parquet file
    parquet_path = tmp_path / "data" / "user_comments.parquet"
    parquet_path.parent.mkdir(parents=True)
    mock_df.to_parquet(parquet_path)
    
    # Update config paths
    producer_config.avro_schema_path = str(schema_path)
    producer_config.review_parquet_path = str(parquet_path)
    
    # Run produce
    with patch("src.producer.produce_refactored.Path") as mock_path:
        mock_path.return_value.parent = tmp_path
        result = produce_comment_events(producer_config, "local")
    
    # Verify
    assert result is True
    mock_admin.create_topic.assert_called_once()
    assert mock_producer.produce.call_count == 2
    mock_producer.flush.assert_called_once()
```

---

### Step 5: Backward Compatibility Strategy

**Option 1: Gradual Migration (Recommended)**

Keep both `produce.py` (old) and `produce_refactored.py` (new) during transition:

```bash
# Old way (still works)
python src/producer/produce.py -m setup -t local

# New way (using shared utilities)
python src/producer/produce_refactored.py -m setup -t local
```

**Option 2: Feature Flag**

Add environment variable to switch between implementations:

```python
# src/producer/produce.py
USE_SHARED_UTILITIES = os.getenv("USE_SHARED_UTILITIES", "false").lower() == "true"

if USE_SHARED_UTILITIES:
    from produce_refactored import main
else:
    # Original implementation
    ...
```

**Option 3: Direct Replacement (After Testing)**

Once verified, replace `produce.py` with refactored version:

```bash
# Backup old version
mv src/producer/produce.py src/producer/produce_legacy.py

# Rename refactored version
mv src/producer/produce_refactored.py src/producer/produce.py
```

---

## Phase 2.2: Migrate CDC Producer Module

### Current State Analysis

**File**: [src/cdc_producer/cdc_produce.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/cdc_producer/cdc_produce.py)

**Code Duplication Identified:**
- ✅ Manual PostgreSQL client initialization (lines 44-52)
- ✅ Manual parquet batch reading (lines 61-96)
- ✅ Manual logging configuration (line 14)
- ✅ Manual environment variable loading (line 25)

**Shared Utilities to Use:**
- `PostgresConfig` - Database configuration
- `PostgresClient` - Connection pooling
- `ParquetBatchReader` - Batch reading
- `setup_logging`, `get_logger` - Centralized logging

---

### Step 1: Create CDC Producer Configuration

#### File: `src/cdc_producer/config.py` (NEW)

```python
"""CDC Producer module configuration"""
from pydantic import Field
from common.config import PostgresConfig


class CDCProducerConfig(PostgresConfig):
    """CDC Producer-specific configuration"""
    
    # Data paths
    review_parquet_path: str = Field(
        default="data/user_comments.parquet",
        description="Path to review parquet file (relative to cdc_producer dir)",
        env="REVIEW_PARQUET_PATH"
    )
    
    # Batch processing
    batch_size: int = Field(
        default=10,
        description="Batch size for event production",
        env="BATCH_SIZE"
    )
    
    # Processing limits
    max_records: int = Field(
        default=100,
        description="Maximum records to process per run",
        env="MAX_RECORDS"
    )
    
    def get_absolute_parquet_path(self, base_dir: str) -> str:
        """Get absolute path to parquet file"""
        import os
        return os.path.join(base_dir, self.review_parquet_path)
```

---

### Step 2: Refactor CDC Producer Module

#### File: `src/cdc_producer/cdc_produce_refactored.py` (NEW)

```python
"""Refactored CDC producer using shared utilities"""
import time
import random
import argparse
from pathlib import Path
from common.logging import setup_logging, get_logger
from common.storage import ParquetBatchReader
from common.db import PostgresClient
from .config import CDCProducerConfig
from db import Event

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
    "-b",
    "--batch_size",
    type=int,
    default=10,
    help="The batch size of the events to produce"
)


def format_event(row):
    """Format row data into Event object"""
    return Event(
        user_id=str(row.get("user_id", "anonymous")),
        comments=str(row.get("comments", ""))
    )


def produce_event(config: CDCProducerConfig):
    """
    Produce events from parquet file to PostgreSQL.
    
    Args:
        config: CDCProducerConfig instance
    """
    base_dir = Path(__file__).parent
    parquet_path = config.get_absolute_parquet_path(str(base_dir))
    
    logger.info(f"Parquet path: {parquet_path}")
    
    # Initialize PostgreSQL client using shared utility
    logger.info("Connecting to PostgreSQL Client")
    pg_client = PostgresClient(config)
    logger.info("PostgreSQL Client connected successfully")
    
    # Initialize Parquet Reader
    logger.info(f"Reading parquet file: {parquet_path}")
    reader = ParquetBatchReader(parquet_path, batch_size=config.batch_size)
    
    # Produce events to PostgreSQL Database
    logger.info("Producing events to PostgreSQL Database... 🔥")
    
    total_processed = 0
    
    try:
        with pg_client.get_session() as session:
            for batch_df in reader.iter_batches(max_records=config.max_records):
                records = []
                
                logger.info(f"Batch length: {len(batch_df)}")
                
                for index, row in batch_df.iterrows():
                    random_stop = random.randint(1, config.batch_size)
                    
                    record = format_event({
                        "user_id": row["reviewerID"],
                        "comments": row["reviewText"]
                    })
                    records.append(record)
                    total_processed += 1
                    
                    if random_stop == index:
                        break
                
                # Bulk insert
                try:
                    session.bulk_save_objects(records)
                    session.commit()
                    logger.info(f"Processed {len(records)} records")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to commit batch to PostgreSQL: {e}")
                    raise
                
                # Random sleep between batches
                time.sleep(random.uniform(1, 10))
                
                if total_processed >= config.max_records:
                    logger.info(f"Total processed records reached {config.max_records}, breaking")
                    break
    
    except Exception as e:
        logger.error(f"Error producing events to PostgreSQL Database: {e}")
        raise
    
    finally:
        pg_client.close()
        logger.info("PostgreSQL Client disconnected successfully")
        logger.info(f"Total processed: {total_processed} records")


def main():
    """Main entry point"""
    args = parser.parse_args()
    
    # Load configuration
    config = CDCProducerConfig()
    config.batch_size = args.batch_size
    
    produce_event(config)


if __name__ == "__main__":
    main()
```

---

### Step 3: Create CDC Producer Tests

#### File: `tests/integration/test_cdc_producer_migration.py` (NEW)

```python
"""Integration test for CDC producer migration"""
import pytest
import os
from unittest.mock import Mock, patch
from src.cdc_producer.cdc_produce_refactored import produce_event, format_event
from src.cdc_producer.config import CDCProducerConfig


@pytest.fixture
def cdc_config():
    """Create test CDC producer configuration"""
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_PORT"] = "5432"
    os.environ["POSTGRES_DB"] = "test_db"
    os.environ["POSTGRES_USER"] = "test_user"
    os.environ["POSTGRES_PASSWORD"] = "test_pass"
    
    return CDCProducerConfig()


def test_cdc_config_loading(cdc_config):
    """Test CDC config loads correctly"""
    assert cdc_config.host == "localhost"
    assert cdc_config.port == 5432
    assert cdc_config.database == "test_db"
    assert cdc_config.batch_size == 10
    assert cdc_config.max_records == 100


def test_format_event():
    """Test event formatting"""
    row = {"user_id": "user123", "comments": "Great product!"}
    event = format_event(row)
    
    assert event.user_id == "user123"
    assert event.comments == "Great product!"


@patch("src.cdc_producer.cdc_produce_refactored.PostgresClient")
@patch("src.cdc_producer.cdc_produce_refactored.ParquetBatchReader")
def test_produce_event_integration(
    mock_reader_class,
    mock_pg_client_class,
    cdc_config,
    tmp_path
):
    """Test produce_event with mocked dependencies"""
    # Mock PostgreSQL client
    mock_pg_client = Mock()
    mock_session = Mock()
    mock_pg_client.get_session.return_value.__enter__.return_value = mock_session
    mock_pg_client_class.return_value = mock_pg_client
    
    # Mock parquet reader
    import pandas as pd
    mock_reader = Mock()
    mock_df = pd.DataFrame({
        "reviewerID": ["user1", "user2"],
        "reviewText": ["Great!", "Nice!"]
    })
    mock_reader.iter_batches.return_value = [mock_df]
    mock_reader_class.return_value = mock_reader
    
    # Create dummy parquet file
    parquet_path = tmp_path / "data" / "user_comments.parquet"
    parquet_path.parent.mkdir(parents=True)
    mock_df.to_parquet(parquet_path)
    
    # Update config
    cdc_config.review_parquet_path = str(parquet_path)
    cdc_config.max_records = 10
    
    # Run produce
    with patch("src.cdc_producer.cdc_produce_refactored.Path") as mock_path:
        mock_path.return_value.parent = tmp_path
        produce_event(cdc_config)
    
    # Verify
    mock_session.bulk_save_objects.assert_called()
    mock_session.commit.assert_called()
    mock_pg_client.close.assert_called_once()
```

---

## Phase 2.3: Migrate Agent Module

### Current State Analysis

**File**: [src/agent/main.py](file:///Users/dannytram/MYSELF/MLOps_K6/final_project/Livestream-Assistant-Agent/src/agent/main.py)

**Code Duplication Identified:**
- ✅ Manual Redis client initialization (lines 24-32)
- ✅ Manual logging configuration (line 14)
- ✅ Manual environment variable loading (lines 19-21)

**Shared Utilities to Use:**
- `RedisConfig` - Redis configuration
- `RedisClientManager` - Connection management
- `setup_logging`, `get_logger` - Centralized logging

---

### Step 1: Create Agent Configuration

#### File: `src/agent/config.py` (NEW)

```python
"""Agent module configuration"""
from pydantic import Field
from common.config import RedisConfig


class AgentConfig(RedisConfig):
    """Agent-specific configuration"""
    
    # Spark NLP settings
    is_apple_silicon: bool = Field(
        default=False,
        description="Whether running on Apple Silicon",
        env="IS_APPLE_SILICON"
    )
    
    # Sentiment model settings
    sentiment_model_name: str = Field(
        default="sentimentdl_use_twitter",
        description="Sentiment model name",
        env="SENTIMENT_MODEL_NAME"
    )
    
    encoder_name: str = Field(
        default="tfhub_use",
        description="Encoder model name",
        env="ENCODER_NAME"
    )
    
    input_col: str = Field(
        default="comments",
        description="Input column name for sentiment analysis",
        env="INPUT_COL"
    )
    
    use_gpu: bool = Field(
        default=False,
        description="Whether to use GPU for inference",
        env="USE_GPU"
    )
    
    # API settings
    api_host: str = Field(
        default="0.0.0.0",
        description="API host",
        env="API_HOST"
    )
    
    api_port: int = Field(
        default=8081,
        description="API port",
        env="API_PORT"
    )
    
    reload: bool = Field(
        default=False,
        description="Enable auto-reload",
        env="API_RELOAD"
    )
    
    workers: int = Field(
        default=1,
        description="Number of worker processes",
        env="API_WORKERS"
    )
```

---

### Step 2: Refactor Agent Module

#### File: `src/agent/main_refactored.py` (NEW)

```python
"""Refactored agent using shared utilities"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from common.logging import setup_logging, get_logger
from common.storage import RedisClientManager
from .config import AgentConfig
from core.sentiment_extract import SentimentExtract
from routers import feature_router, sentiment_router

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Load configuration
config = AgentConfig()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager"""
    # Initialize Redis client using shared utility
    logger.info("Initializing Redis client...")
    app.state.redis_client = await RedisClientManager.create(config)
    logger.info("Redis client initialized")
    
    # Initialize sentiment service
    logger.info("Initializing sentiment service...")
    app.state.sentiment_service = SentimentExtract(
        input_col=config.input_col,
        model_name=config.sentiment_model_name,
        encoder_name=config.encoder_name,
        gpu=config.use_gpu,
        apple_silicon=config.is_apple_silicon
    )
    logger.info("Sentiment service initialized")
    
    yield
    
    # Cleanup
    await app.state.redis_client.close()
    logger.info("Redis client closed")


# Create FastAPI app
app = FastAPI(title="Feature Retrieval Service", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    """Health check endpoint"""
    return JSONResponse(status_code=200, content={"status": "healthy"})


# Include routers
app.include_router(
    feature_router,
    prefix="/api/feature_extraction",
    tags=["feature_extraction"],
)

app.include_router(
    sentiment_router,
    prefix="/api/sentiment",
    tags=["sentiment"],
)


def main():
    """Main entry point"""
    uvicorn.run(
        "main_refactored:app",
        host=config.api_host,
        port=config.api_port,
        reload=config.reload,
        workers=config.workers
    )


if __name__ == "__main__":
    main()
```

---

### Step 3: Create Agent Tests

#### File: `tests/integration/test_agent_migration.py` (NEW)

```python
"""Integration test for agent migration"""
import pytest
import os
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient


@pytest.fixture
def agent_config():
    """Create test agent configuration"""
    os.environ["REDIS_HOST"] = "localhost"
    os.environ["REDIS_PORT"] = "6379"
    os.environ["IS_APPLE_SILICON"] = "false"
    
    from src.agent.config import AgentConfig
    return AgentConfig()


def test_agent_config_loading(agent_config):
    """Test agent config loads correctly"""
    assert agent_config.host == "localhost"
    assert agent_config.port == 6379
    assert agent_config.is_apple_silicon is False
    assert agent_config.sentiment_model_name == "sentimentdl_use_twitter"


@pytest.mark.asyncio
@patch("src.agent.main_refactored.RedisClientManager")
@patch("src.agent.main_refactored.SentimentExtract")
async def test_app_lifespan(mock_sentiment_class, mock_redis_manager_class):
    """Test FastAPI lifespan with mocked dependencies"""
    # Mock Redis client
    mock_redis_client = AsyncMock()
    mock_redis_manager_class.create.return_value = mock_redis_client
    
    # Mock sentiment service
    mock_sentiment = Mock()
    mock_sentiment_class.return_value = mock_sentiment
    
    # Import app
    from src.agent.main_refactored import app
    
    # Test client
    with TestClient(app) as client:
        # Test health endpoint
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}
    
    # Verify Redis client was created and closed
    mock_redis_manager_class.create.assert_called_once()
    mock_redis_client.close.assert_called_once()


def test_api_endpoints():
    """Test that API endpoints are registered"""
    from src.agent.main_refactored import app
    
    routes = [route.path for route in app.routes]
    
    assert "/health" in routes
    assert "/api/feature_extraction" in [r for r in routes if r.startswith("/api/feature")]
    assert "/api/sentiment" in [r for r in routes if r.startswith("/api/sentiment")]
```

---

## Verification Plan

### 1. Unit Tests

Run all unit tests from Phase 1 plus new integration tests:

```bash
# Run all tests
pytest tests/ -v

# Run specific module tests
pytest tests/integration/test_producer_migration.py -v
pytest tests/integration/test_cdc_producer_migration.py -v
pytest tests/integration/test_agent_migration.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

### 2. Integration Tests

#### Producer Module

```bash
# Test old implementation
python src/producer/produce.py -m setup -t local

# Test new implementation
python src/producer/produce_refactored.py -m setup -t local

# Verify both produce same results
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic <TOPIC_NAME> \
  --from-beginning \
  --max-messages 10
```

#### CDC Producer Module

```bash
# Test old implementation
python src/cdc_producer/cdc_produce.py -b 10

# Test new implementation
python src/cdc_producer/cdc_produce_refactored.py -b 10

# Verify database records
psql -h localhost -U <user> -d <database> -c "SELECT COUNT(*) FROM events;"
```

#### Agent Module

```bash
# Test old implementation
python src/agent/main.py

# Test new implementation
python src/agent/main_refactored.py

# Test health endpoint
curl http://localhost:8081/health

# Test feature extraction endpoint
curl -X POST http://localhost:8081/api/feature_extraction \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test_user"}'
```

### 3. Performance Comparison

Create benchmark script to compare old vs new implementations:

#### File: `tests/benchmarks/compare_implementations.py` (NEW)

```python
"""Benchmark old vs new implementations"""
import time
import subprocess
import statistics


def benchmark_producer(implementation: str, runs: int = 3):
    """Benchmark producer implementation"""
    times = []
    
    for i in range(runs):
        start = time.time()
        
        cmd = f"python src/producer/{implementation}.py -m setup -t local"
        subprocess.run(cmd, shell=True, check=True)
        
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"Run {i+1}: {elapsed:.2f}s")
    
    return {
        "mean": statistics.mean(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times)
    }


if __name__ == "__main__":
    print("Benchmarking old implementation...")
    old_stats = benchmark_producer("produce")
    
    print("\nBenchmarking new implementation...")
    new_stats = benchmark_producer("produce_refactored")
    
    print("\n=== Results ===")
    print(f"Old: {old_stats['mean']:.2f}s ± {old_stats['stdev']:.2f}s")
    print(f"New: {new_stats['mean']:.2f}s ± {new_stats['stdev']:.2f}s")
    
    improvement = ((old_stats['mean'] - new_stats['mean']) / old_stats['mean']) * 100
    print(f"Improvement: {improvement:+.1f}%")
```

Run benchmark:

```bash
python tests/benchmarks/compare_implementations.py
```

### 4. Manual Verification Checklist

- [ ] Producer module produces same number of records
- [ ] CDC producer module inserts same data into PostgreSQL
- [ ] Agent module responds to all API endpoints
- [ ] No errors in logs during migration
- [ ] Configuration loads correctly from `.env` files
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Performance is comparable or better
- [ ] Docker containers still work with new code

---

## Rollback Plan

If issues are discovered during migration:

### Option 1: Keep Both Implementations

```bash
# Revert to old implementation
git checkout main -- src/producer/produce.py
git checkout main -- src/cdc_producer/cdc_produce.py
git checkout main -- src/agent/main.py
```

### Option 2: Feature Flag

Add environment variable to switch implementations:

```bash
# Use old implementation
export USE_SHARED_UTILITIES=false

# Use new implementation
export USE_SHARED_UTILITIES=true
```

### Option 3: Git Revert

```bash
# Revert entire migration
git revert <migration-commit-hash>
```

---

## Migration Checklist

### Phase 2.1: Producer Module
- [ ] Create `src/producer/config.py`
- [ ] Create `src/producer/produce_refactored.py`
- [ ] Create `tests/integration/test_producer_migration.py`
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Benchmark performance
- [ ] Update documentation
- [ ] Deploy to staging
- [ ] Verify in production

### Phase 2.2: CDC Producer Module
- [ ] Create `src/cdc_producer/config.py`
- [ ] Create `src/cdc_producer/cdc_produce_refactored.py`
- [ ] Create `tests/integration/test_cdc_producer_migration.py`
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Verify database writes
- [ ] Update documentation
- [ ] Deploy to staging
- [ ] Verify in production

### Phase 2.3: Agent Module
- [ ] Create `src/agent/config.py`
- [ ] Create `src/agent/main_refactored.py`
- [ ] Create `tests/integration/test_agent_migration.py`
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Test all API endpoints
- [ ] Update documentation
- [ ] Deploy to staging
- [ ] Verify in production

---

## Success Criteria

Phase 2 is considered complete when:

1. ✅ All three modules migrated to use shared utilities
2. ✅ All unit tests pass (100% coverage for new code)
3. ✅ All integration tests pass
4. ✅ Performance is comparable or better than original
5. ✅ No breaking changes to existing functionality
6. ✅ Documentation updated
7. ✅ Code review approved
8. ✅ Successfully deployed to production

---

## Next Steps: Phase 3 (Future)

Phase 3 will focus on:
1. Schema consolidation (Avro schemas)
2. Data file organization
3. Advanced monitoring and observability
4. Performance optimizations
5. Additional shared utilities (e.g., metrics, tracing)

This will be detailed after Phase 2 is complete and approved.
