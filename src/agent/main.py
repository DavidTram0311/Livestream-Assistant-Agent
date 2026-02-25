"""
DEPRECATED: This module has been integrated into the unified API server.
Please use src/api/server.py instead.

This file is kept for backward compatibility but will redirect to the unified API.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from common.logging import setup_logging, get_logger
from common.storage import RedisClientManager
from .config import AgentConfig
from .core.sentiment_extract import SentimentExtract
from .routers import feature_router, sentiment_router
from pathlib import Path

# Setup logging
setup_logging(level="INFO")
logger = get_logger(__name__)

logger.warning("⚠️  DEPRECATED: agent/main.py is deprecated. Use api/server.py for the unified API service.")

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
    """
    Main entry point - DEPRECATED
    
    This standalone agent service has been integrated into the unified API server.
    Please use: python -m api.server
    """
    logger.warning("=" * 80)
    logger.warning("⚠️  DEPRECATION WARNING")
    logger.warning("=" * 80)
    logger.warning("The standalone agent service (agent/main.py) is deprecated.")
    logger.warning("All agent endpoints have been integrated into the unified API server.")
    logger.warning("")
    logger.warning("Please use the unified API server instead:")
    logger.warning("  python -m api.server")
    logger.warning("")
    logger.warning("Or if running from main.py:")
    logger.warning("  python main.py")
    logger.warning("=" * 80)
    
    # Still run the service for backward compatibility
    logger.info("Starting deprecated agent service on port 8001...")
    logger.info("Note: The unified API server runs on port 8000")
    
    uvicorn.run(
        "agent.main:app",
        host=config.api_host,
        port=8001,  # Changed to avoid conflict with unified API
        reload=config.reload,
        workers=config.workers
    )


if __name__ == "__main__":
    main()