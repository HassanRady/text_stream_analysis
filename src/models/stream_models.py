from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Stream(Base):
    __tablename__ = "streams"

    id = Column(String(36), primary_key=True)
    subreddit = Column(String(255), unique=True, nullable=False, index=True)
    status = Column(
        String(50),
        nullable=False,
        default="inactive",
        index=True,
    )  # inactive, starting, active, paused, stopped, error
    instance_id = Column(String(255), nullable=True, index=True)
    config = Column(JSON, default={})
    last_heartbeat = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )


class StreamCheckpoint(Base):
    __tablename__ = "stream_checkpoints"

    id = Column(String(36), primary_key=True)
    stream_id = Column(String(36), unique=True, nullable=False, index=True)
    last_comment_id = Column(String(255), nullable=True)
    last_processed_at = Column(DateTime, nullable=True)
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )


class StreamError(Base):
    __tablename__ = "stream_errors"

    id = Column(String(36), primary_key=True)
    stream_id = Column(String(36), nullable=False, index=True)
    error_type = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    is_recoverable = Column(Integer, default=1)  # Boolean (1/0)
    timestamp = Column(DateTime, default=func.now(), nullable=False)
