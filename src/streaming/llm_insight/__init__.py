"""
LLM Insight Extraction Service

This module provides a Kafka-based streaming pipeline that:
1. Consumes aggregated gender and sentiment statistics from ksqlDB
2. Merges statistics by window timestamps
3. Generates insights using OpenAI
4. Produces insights to streaming.llm_insights topic
"""
from .models import CombinedStats, LLMInsight, LLMResponse
from .client import OpenAIClient
from .processor import LLMInsightProcessor

__all__ = [
    "CombinedStats",
    "LLMInsight",
    "LLMResponse",
    "OpenAIClient",
    "LLMInsightProcessor",
]
