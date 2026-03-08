"""OpenAI client wrapper for LLM insight generation"""
import asyncio
import json
from typing import Optional, Tuple, List
from openai import AsyncOpenAI
from openai import APIError, RateLimitError, APIConnectionError

from src.common.logging import get_logger
from src.common.config import LLMInsightConfig
from .models import CombinedStats, LLMResponse

logger = get_logger(__name__)

SYSTEM_PROMPT = """You are an AI assistant helping livestreamers understand their audience engagement in real-time.

Your task is to analyze aggregated statistics from a 2-minute window of livestream comments and provide:
1. A concise summary of audience demographics and sentiment
2. 2-3 actionable recommendations for the livestreamer

Guidelines:
- Be concise and direct - livestreamers need quick insights
- Focus on notable patterns or anomalies
- Recommendations should be specific and actionable
- If sentiment is trending negative, suggest ways to address it
- Consider gender distribution for content relevance
- If data is partial or limited, acknowledge it briefly

Respond in JSON format with exactly these fields:
{
    "summary": "A 2-3 sentence summary of the audience engagement",
    "recommendations": ["Recommendation 1", "Recommendation 2", "Recommendation 3"]
}"""

USER_PROMPT_TEMPLATE = """Analyze the following livestream statistics and provide insights:

{context}

Provide a summary and 2-3 actionable recommendations in JSON format."""


class OpenAIClient:
    """
    Async OpenAI client for generating livestream insights.
    Includes retry logic with exponential backoff.
    """
    
    def __init__(self, config: LLMInsightConfig):
        self.config = config
        self.client: Optional[AsyncOpenAI] = None
        self._total_tokens_used = 0
        self._total_requests = 0
    
    async def __aenter__(self):
        """Async context manager entry"""
        if self.config.openai_api_key:
            self.client = AsyncOpenAI(api_key=self.config.openai_api_key)
        else:
            logger.warning("OpenAI API key not configured. Insights will use fallback generation.")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.client:
            await self.client.close()
        logger.info(
            f"OpenAI client closed. Total requests: {self._total_requests}, "
            f"Total tokens: {self._total_tokens_used}"
        )
    
    async def generate_insight(
        self,
        stats: CombinedStats
    ) -> Tuple[str, List[str]]:
        """
        Generate insights for a window using OpenAI.
        
        Args:
            stats: Combined statistics for the window
            
        Returns:
            Tuple of (summary, recommendations)
        """
        if not self.client:
            return self._generate_fallback_insight(stats)
        
        context = stats.to_prompt_context()
        user_prompt = USER_PROMPT_TEMPLATE.format(context=context)
        
        for attempt in range(self.config.max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=self.config.openai_model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=self.config.openai_max_tokens,
                    temperature=self.config.openai_temperature,
                    response_format={"type": "json_object"}
                )
                
                self._total_requests += 1
                if response.usage:
                    self._total_tokens_used += response.usage.total_tokens
                    logger.debug(
                        f"Token usage - prompt: {response.usage.prompt_tokens}, "
                        f"completion: {response.usage.completion_tokens}"
                    )
                
                content = response.choices[0].message.content
                return self._parse_response(content)
                
            except RateLimitError as e:
                delay = self.config.retry_delay_seconds * (2 ** attempt)
                logger.warning(f"Rate limit hit, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
                
            except APIConnectionError as e:
                delay = self.config.retry_delay_seconds * (2 ** attempt)
                logger.warning(f"Connection error, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
                
            except APIError as e:
                logger.error(f"OpenAI API error: {e}")
                if attempt == self.config.max_retries - 1:
                    return self._generate_fallback_insight(stats)
                delay = self.config.retry_delay_seconds * (2 ** attempt)
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Unexpected error generating insight: {e}")
                return self._generate_fallback_insight(stats)
        
        return self._generate_fallback_insight(stats)
    
    def _parse_response(
        self,
        content: str,
    ) -> Tuple[str, List[str]]:
        """Parse LLM response into summary and recommendations"""
        try:
            data = json.loads(content)
            llm_response = LLMResponse(**data)
            return llm_response.summary, llm_response.recommendations
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
            return content.strip(), []
    
    def _generate_fallback_insight(
        self,
        stats: CombinedStats
    ) -> Tuple[str, List[str]]:
        """Generate a basic insight without LLM when API is unavailable"""
        logger.info("Using fallback insight generation")
        
        summary_parts = []
        recommendations = []
        
        total = stats.total_count
        summary_parts.append(f"Analyzed {total} comments in this 5-minute window.")
        
        gender_dist = stats.gender_distribution()
        dominant_gender = max(gender_dist, key=gender_dist.get)
        if dominant_gender != "unknown":
            summary_parts.append(f"Audience is predominantly {dominant_gender} ({gender_dist[dominant_gender]}%).")
        
        sentiment_dist = stats.sentiment_distribution()
        if sentiment_dist["positive"] > sentiment_dist["negative"]:
            summary_parts.append(f"Sentiment is mostly positive ({sentiment_dist['positive']}%).")
            recommendations.append("Keep up the current engagement style - audience is responding well.")
        elif sentiment_dist["negative"] > sentiment_dist["positive"]:
            summary_parts.append(f"Sentiment is trending negative ({sentiment_dist['negative']}%).")
            recommendations.append("Consider addressing audience concerns or changing topic.")
            recommendations.append("Engage directly with viewers to understand their feedback.")
        else:
            summary_parts.append("Sentiment is mixed.")
            recommendations.append("Monitor sentiment trends in upcoming windows.")
        
        if not recommendations:
            recommendations.append("Continue monitoring audience engagement.")
        
        return " ".join(summary_parts), recommendations
    
    @property
    def stats(self) -> dict:
        """Return usage statistics"""
        return {
            "total_requests": self._total_requests,
            "total_tokens": self._total_tokens_used,
        }
