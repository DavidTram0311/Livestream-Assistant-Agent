from core.sentiment_extract import SentimentExtract
import logging
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

class SentimentRequest(BaseModel):
    text: str
    comment_id: int

feature_router = APIRouter()
sentiment_router = APIRouter()

@feature_router.get("/gender/{user_id}")
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

@sentiment_router.post("/")
async def get_sentiment(
    payload: SentimentRequest,
    request: Request
):
    sentiment_service = request.app.state.sentiment_service
    text = payload.text
    comment_id = payload.comment_id

    if text is None or comment_id is None:
        raise HTTPException(status_code=400, detail="Text and comment_id are required")
        
    try:
        sentiment = sentiment_service.predict(text)
    except Exception as e:
        logging.error(f"Sentiment error: {e}")
        raise HTTPException(status_code=500, detail="Sentiment error")
    
    return JSONResponse(status_code=200, content={
        "comment_id": comment_id,
        "sentiment": sentiment
        })