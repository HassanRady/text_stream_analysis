"""Unit tests for CheckpointFlusher."""

import uuid
from unittest.mock import AsyncMock

import pytest

from src.tasks.checkpoint_flusher import CheckpointFlusher


class TestCheckpointFlusherFlush:
    """Test flush method."""

    @pytest.mark.asyncio
    async def test_flush_no_checkpoints(self, redis_mock, session_maker_mock):
        redis_mock.keys = AsyncMock(return_value=[])
        flusher = CheckpointFlusher(
            redis_client=redis_mock, session_maker=session_maker_mock
        )
        await flusher.flush()
        redis_mock.keys.assert_called_once_with("stream:checkpoint:*")

    @pytest.mark.asyncio
    async def test_flush_single_checkpoint(
        self, redis_mock, session_mock, session_maker_mock
    ):
        stream_id = str(uuid.uuid4())
        checkpoint_key = f"stream:checkpoint:{stream_id}"
        redis_mock.keys = AsyncMock(return_value=[checkpoint_key])
        redis_mock.hgetall = AsyncMock(
            return_value={
                "last_comment_id": "abc123",
                "last_processed_at": "2026-05-05T10:00:00Z",
            }
        )
        flusher = CheckpointFlusher(
            redis_client=redis_mock, session_maker=session_maker_mock
        )
        await flusher.flush()
        redis_mock.keys.assert_called_once()
        session_mock.execute.assert_called()

    @pytest.mark.asyncio
    async def test_flush_strips_timezone(
        self, redis_mock, session_mock, session_maker_mock
    ):
        stream_id = str(uuid.uuid4())
        checkpoint_key = f"stream:checkpoint:{stream_id}"
        redis_mock.keys = AsyncMock(return_value=[checkpoint_key])
        redis_mock.hgetall = AsyncMock(
            return_value={
                "last_comment_id": "abc123",
                "last_processed_at": "2026-05-05T10:00:00+00:00",
            }
        )
        flusher = CheckpointFlusher(
            redis_client=redis_mock, session_maker=session_maker_mock
        )
        await flusher.flush()
        call_args = session_mock.execute.call_args
        params = call_args[0][1]
        ts = params.get("last_processed_at")
        assert ts.tzinfo is None
