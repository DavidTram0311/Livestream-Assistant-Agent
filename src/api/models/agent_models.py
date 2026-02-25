from pydantic import BaseModel

class SentimentRequest(BaseModel):
    text: str
    comment_id: int

class SentimentResponse(BaseModel):
    sentiment: str
    comment_id: int

class GenderResponse(BaseModel):
    gender: str
    user_id: str