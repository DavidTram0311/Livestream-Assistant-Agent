"""Refactored agent using shared utilities"""
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