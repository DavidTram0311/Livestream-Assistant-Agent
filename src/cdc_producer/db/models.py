from .base import Base
from sqlalchemy import Column, Integer, String, DateTime, func

class Event(Base):
    __tablename__ = "comment_events"
    comment_id = Column(
        Integer,
        primary_key=True
    )
    user_id = Column(
        String,
        nullable=False
    )
    comments = Column(
        String,
        nullable=False
    )
    event_timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.timezone("UTC", func.current_timestamp()),
    )

