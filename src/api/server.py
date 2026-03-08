"""Feature & Sentiment API Server"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from src.common.logging import setup_logging, get_logger
from src.api.routers import feature_router, sentiment_router
from contextlib import asynccontextmanager
from src.common.storage import RedisClientManager
from src.agent.core.sentiment_extract import SentimentExtract
from src.agent.config import AgentConfig
from src.agent.feature_source import push_parquet_to_redis
from pathlib import Path

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

# Load configuration
config = AgentConfig()

# Ensure SparkNLP cache directory exists
cache_path = Path(config.sparknlp_cache_folder)
cache_path.mkdir(parents=True, exist_ok=True)
logger.info(f"SparkNLP cache directory: {config.sparknlp_cache_folder}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager"""
    # Initialize Redis client using shared utility
    logger.info("Initializing Redis client...")
    app.state.redis_client = await RedisClientManager.create(config)
    logger.info("Redis client initialized")
    
    # Clear Redis and push user table parquet
    logger.info("Clearing Redis and pushing parquet...")
    await app.state.redis_client.flushall()
    await push_parquet_to_redis(app.state.redis_client, config.user_table_path)
    logger.info("Redis initialization complete")
    
    # Initialize sentiment service
    logger.info("Initializing sentiment service...")
    app.state.sentiment_service = SentimentExtract(
        input_col=config.input_col,
        model_name=config.sentiment_model_name,
        encoder_name=config.encoder_name,
        gpu=config.use_gpu,
        apple_silicon=config.is_apple_silicon,
        cache_folder=config.sparknlp_cache_folder
    )
    logger.info("Sentiment service initialized")
    
    yield
    
    # Cleanup
    await app.state.redis_client.close()
    logger.info("Redis client closed")

# Create FastAPI app
app = FastAPI(
    title="Feature & Sentiment API Service",
    description="API service for feature extraction and sentiment analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Root endpoint"""
    return JSONResponse(
        status_code=200,
        content={
            "service": "Feature & Sentiment API Service",
            "version": "1.0.0",
            "status": "running"
        }
    )


@app.get("/health")
def health():
    """Health check endpoint"""
    return JSONResponse(
        status_code=200,
        content={"status": "healthy"}
    )


# Include Feature Extraction router
app.include_router(
    feature_router,
    prefix="/api/feature_extraction",
    tags=["feature_extraction"],
)

# Include Sentiment router
app.include_router(
    sentiment_router,
    prefix="/api/sentiment",
    tags=["sentiment"],
)


def main():
    """Main entry point"""
    logger.info("Starting Feature & Sentiment API Service...")
    uvicorn.run(
        "api.server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1
    )


if __name__ == "__main__":
    main()
