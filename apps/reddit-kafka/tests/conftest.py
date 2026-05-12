import os
from unittest.mock import AsyncMock, MagicMock

import asyncpraw
import pytest
import redis.asyncio as redis


@pytest.fixture(scope="session")
def reddit_client():
    yield asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT"),
    )


@pytest.fixture
def redis_mock():
    """Mock Redis client for testing with async support."""
    mock = AsyncMock(spec=redis.Redis)
    mock.set = AsyncMock()
    mock.get = AsyncMock()
    mock.hset = AsyncMock()
    mock.hgetall = AsyncMock()
    mock.keys = AsyncMock()
    mock.delete = AsyncMock()
    mock.lpush = AsyncMock()
    mock.ltrim = AsyncMock()
    mock.expire = AsyncMock()
    mock.llen = AsyncMock()
    mock.lrange = AsyncMock()
    mock.pipeline = AsyncMock()
    return mock


@pytest.fixture
def session_mock():
    """Mock SQLAlchemy async session."""
    mock = AsyncMock()
    mock.execute = AsyncMock()
    mock.commit = AsyncMock()
    mock.rollback = AsyncMock()
    mock.__aenter__ = AsyncMock(return_value=mock)
    mock.__aexit__ = AsyncMock(return_value=None)
    return mock


class AsyncContextManagerMock:
    """Helper class to create proper async context managers in tests."""

    def __init__(self, session_mock):
        self.session_mock = session_mock

    async def __aenter__(self):
        return self.session_mock

    async def __aexit__(self, *args):
        pass


@pytest.fixture
def session_maker_mock(session_mock):
    """Mock SQLAlchemy async session maker that supports assertions
    and async context manager."""
    return MagicMock(side_effect=lambda: AsyncContextManagerMock(session_mock))


# Enable pytest-asyncio
pytest_plugins = ("pytest_asyncio",)
