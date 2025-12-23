from .base import Base
from sqlalchemy import Column, String, BigInteger, text
import time

class Event(Base):
    __tablename__ = "comment_events"
    comment_id = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True
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
        BigInteger,
        nullable=False,
        server_default=text("(EXTRACT(EPOCH FROM now()) * 1000)::bigint")
    )

