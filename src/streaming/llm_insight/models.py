"""Pydantic models for LLM Insight service"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime


class CombinedStats(BaseModel):
    """
    Combined gender and sentiment statistics from ksqlDB aggregation (streaming.combined_stats topic).
    Represents a 5-minute tumbling window aggregation with both gender and sentiment counts.
    """
    agg_key: str = Field(default="all", description="Aggregation key")
    window_start: int = Field(..., description="Window start timestamp in epoch milliseconds")
    window_end: int = Field(..., description="Window end timestamp in epoch milliseconds")
    total_count: int = Field(..., description="Total number of comments in window")
    # Gender counts
    male_count: int = Field(default=0, description="Count of male users")
    female_count: int = Field(default=0, description="Count of female users")
    gender_unknown_count: int = Field(default=0, description="Count of unknown gender")
    # Sentiment counts
    positive_count: int = Field(default=0, description="Count of positive sentiment")
    negative_count: int = Field(default=0, description="Count of negative sentiment")
    sentiment_unknown_count: int = Field(default=0, description="Count of unknown sentiment")
    
    @property
    def window_key(self) -> tuple:
        """Unique key for this window"""
        return (self.window_start, self.window_end)
    
    def gender_distribution(self) -> Dict[str, float]:
        """Convert gender counts to percentage distribution"""
        if self.total_count == 0:
            return {"male": 0.0, "female": 0.0, "unknown": 0.0}
        return {
            "male": round(self.male_count / self.total_count * 100, 1),
            "female": round(self.female_count / self.total_count * 100, 1),
            "unknown": round(self.gender_unknown_count / self.total_count * 100, 1),
        }
    
    def sentiment_distribution(self) -> Dict[str, float]:
        """Convert sentiment counts to percentage distribution"""
        if self.total_count == 0:
            return {"positive": 0.0, "negative": 0.0, "unknown": 0.0}
        return {
            "positive": round(self.positive_count / self.total_count * 100, 1),
            "negative": round(self.negative_count / self.total_count * 100, 1),
            "unknown": round(self.sentiment_unknown_count / self.total_count * 100, 1),
        }
    
    def to_prompt_context(self) -> str:
        """Generate context string for LLM prompt"""
        gender_dist = self.gender_distribution()
        sentiment_dist = self.sentiment_distribution()
        
        lines = [
            f"Time Window: {datetime.fromtimestamp(self.window_start / 1000).strftime('%Y-%m-%d %H:%M')} to {datetime.fromtimestamp(self.window_end / 1000).strftime('%H:%M')}",
            f"Total Comments: {self.total_count}",
            f"Gender Distribution: Male {gender_dist['male']}%, Female {gender_dist['female']}%, Unknown {gender_dist['unknown']}%",
            f"Sentiment Distribution: Positive {sentiment_dist['positive']}%, Negative {sentiment_dist['negative']}%, Unknown {sentiment_dist['unknown']}%",
        ]
        
        return "\n".join(lines)


class LLMInsight(BaseModel):
    """
    LLM-generated insight for a window.
    Published to streaming.llm_insights topic.
    """
    window_start: int = Field(..., description="Window start timestamp in epoch milliseconds")
    window_end: int = Field(..., description="Window end timestamp in epoch milliseconds")
    total_comments: int = Field(..., description="Total number of comments analyzed")
    gender_distribution: Dict[str, float] = Field(
        default_factory=dict,
        description="Gender distribution as percentages"
    )
    sentiment_distribution: Dict[str, float] = Field(
        default_factory=dict,
        description="Sentiment distribution as percentages"
    )
    summary: str = Field(..., description="LLM-generated summary of the window")
    recommendations: List[str] = Field(
        default_factory=list,
        description="LLM-generated actionable recommendations"
    )
    generated_at: int = Field(
        default_factory=lambda: int(datetime.now().timestamp() * 1000),
        description="Timestamp when insight was generated (epoch milliseconds)"
    )
    model: str = Field(default="gpt-4o", description="LLM model used for generation")
    
    @classmethod
    def from_combined_stats(
        cls,
        stats: CombinedStats,
        summary: str,
        recommendations: List[str],
        model: str = "gpt-4o",
    ) -> "LLMInsight":
        """Create an LLMInsight from CombinedStats and LLM response"""
        return cls(
            window_start=stats.window_start,
            window_end=stats.window_end,
            total_comments=stats.total_count,
            gender_distribution=stats.gender_distribution(),
            sentiment_distribution=stats.sentiment_distribution(),
            summary=summary,
            recommendations=recommendations,
            model=model,
        )


class LLMResponse(BaseModel):
    """Structured response from LLM for parsing"""
    summary: str = Field(..., description="Summary of the livestream window")
    recommendations: List[str] = Field(..., description="List of actionable recommendations")
