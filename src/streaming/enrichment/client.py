"""Async HTTP client for API enrichment calls"""
import httpx
import asyncio
from typing import Optional, Tuple
from src.common.logging import get_logger
from src.streaming.config import StreamingConfig
from src.streaming.models import (
    GenderAPIResponse,
    SentimentAPIRequest,
    SentimentAPIResponse
)

logger = get_logger(__name__)


class EnrichmentClient:
    """
    Async HTTP client for calling gender and sentiment APIs.
    Includes retry logic and caching for gender lookups.
    """
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.base_url = config.api_base_url.rstrip("/")
        self.timeout = httpx.Timeout(config.api_timeout_seconds)
        self.max_retries = config.api_max_retries
        self._client: Optional[httpx.AsyncClient] = None
        self._gender_cache: dict[str, str] = {}
    
    async def __aenter__(self) -> "EnrichmentClient":
        """Async context manager entry"""
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def get_gender(self, user_id: str) -> str:
        """
        Get gender for a user ID from the API.
        Results are cached since gender is static per user.
        
        Args:
            user_id: User identifier
            
        Returns:
            Gender string (male/female/unknown)
        """
        if user_id in self._gender_cache:
            return self._gender_cache[user_id]
        
        url = f"{self.base_url}/api/feature_extraction/gender/{user_id}"
        
        for attempt in range(self.max_retries):
            try:
                response = await self._client.get(url)
                
                if response.status_code == 200:
                    data = GenderAPIResponse(**response.json())
                    self._gender_cache[user_id] = data.gender
                    return data.gender
                elif response.status_code == 404:
                    logger.debug(f"User {user_id} not found in gender API")
                    self._gender_cache[user_id] = "unknown"
                    return "unknown"
                else:
                    logger.warning(
                        f"Gender API returned {response.status_code} for user {user_id}"
                    )
                    
            except httpx.TimeoutException:
                logger.warning(
                    f"Gender API timeout for user {user_id} (attempt {attempt + 1}/{self.max_retries})"
                )
            except httpx.RequestError as e:
                logger.warning(
                    f"Gender API request error for user {user_id}: {e} (attempt {attempt + 1}/{self.max_retries})"
                )
            except Exception as e:
                logger.error(f"Unexpected error in gender API call: {e}")
                break
            
            if attempt < self.max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
        
        return "unknown"
    
    async def get_sentiment(self, text: str, comment_id: int) -> str:
        """
        Get sentiment prediction for text from the API.
        
        Args:
            text: Comment text to analyze
            comment_id: Comment identifier
            
        Returns:
            Sentiment string (positive/negative/unknown)
        """
        url = f"{self.base_url}/api/sentiment/"
        payload = SentimentAPIRequest(text=text, comment_id=comment_id)
        
        for attempt in range(self.max_retries):
            try:
                response = await self._client.post(
                    url,
                    json=payload.model_dump()
                )
                
                if response.status_code == 200:
                    data = SentimentAPIResponse(**response.json())
                    return data.sentiment
                else:
                    logger.warning(
                        f"Sentiment API returned {response.status_code} for comment {comment_id}"
                    )
                    
            except httpx.TimeoutException:
                logger.warning(
                    f"Sentiment API timeout for comment {comment_id} (attempt {attempt + 1}/{self.max_retries})"
                )
            except httpx.RequestError as e:
                logger.warning(
                    f"Sentiment API request error for comment {comment_id}: {e} (attempt {attempt + 1}/{self.max_retries})"
                )
            except Exception as e:
                logger.error(f"Unexpected error in sentiment API call: {e}")
                break
            
            if attempt < self.max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
        
        return "unknown"
    
    async def enrich(self, user_id: str, text: str, comment_id: int) -> Tuple[str, str]:
        """
        Enrich an event with both gender and sentiment in parallel.
        
        Args:
            user_id: User identifier for gender lookup
            text: Comment text for sentiment analysis
            comment_id: Comment identifier
            
        Returns:
            Tuple of (gender, sentiment)
        """
        gender_task = asyncio.create_task(self.get_gender(user_id))
        sentiment_task = asyncio.create_task(self.get_sentiment(text, comment_id))
        
        gender, sentiment = await asyncio.gather(gender_task, sentiment_task)
        return gender, sentiment
    
    def clear_cache(self):
        """Clear the gender cache"""
        self._gender_cache.clear()
        logger.info("Gender cache cleared")
