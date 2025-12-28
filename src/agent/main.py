import os
import pyarrow.parquet as pq
import redis.asyncio as redis
import logging
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from core.sentiment_extract import SentimentExtract
from routers import feature_router, sentiment_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
IS_APPLE_SILICON = os.getenv("IS_APPLE_SILICON", True)

# Redis init
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis_client = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5
        )
    logging.info(f"Redis client initialized")

    app.state.sentiment_service = SentimentExtract(
    input_col="comments",
    model_name="sentimentdl_use_twitter",
    encoder_name="tfhub_use",
    gpu=False,
    apple_silicon=IS_APPLE_SILICON
    )
    logging.info(f"Sentiment service initialized")
    yield
    await app.state.redis_client.close()
    logging.info(f"Redis client closed")

app = FastAPI(title="Feature Retrieval Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
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


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0", 
        port=8081,
        reload=False,
        workers=1
        )
