import os
import pyarrow.parquet as pq
import redis.asyncio as redis
import logging
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRouter
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from fastapi.requests import Request
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

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

router = APIRouter()

@router.get("/gender/{user_id}")
async def get_gender_by_user(
    user_id: str,
    request: Request
):
    redis_client = request.app.state.redis_client
    
    try:
    # HGET retrieves a specific field from the hash
        gender = await redis_client.hget("user_genders", str(user_id))
    except Exception as e:
        logging.error(f"Redis error: {e}")
        raise HTTPException(status_code=500, detail="Redis error")

    if gender is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return JSONResponse(status_code=200, content={
        "gender": gender,
        "user_id": user_id
        })



@router.get("/health")
def health():
    return JSONResponse(status_code=200, content={"status": "healthy"})

# Include routers
app.include_router(
    router,
    prefix="/api/feature_extraction",
    tags=["feature_extraction"],
    )


if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0", 
        port=8081,
        reload=False,
        workers=1
        )

