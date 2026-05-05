"""Integration tests for timestamp consistency."""

from ast import literal_eval
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from src.repositories.stream_registry import _now_iso
from src.stream.error_handler import ErrorHandler


class TestTimestampConsistency:
    """Test that timestamps are consistently naive UTC."""

    def test_now_iso_returns_naive_utc(self):
        """Verify _now_iso returns naive UTC ISO format."""
        ts = _now_iso()
        # Should end with Z
        assert ts.endswith("Z")
        # Parse and verify naive
        parsed = datetime.fromisoformat(ts.rstrip("Z"))
        assert parsed.tzinfo is None
        # Should be close to now
        now_naive = datetime.now(UTC).replace(tzinfo=None)
        diff = (now_naive - parsed).total_seconds()
        assert 0 <= diff <= 1

    @pytest.mark.asyncio
    async def test_error_handler_uses_naive_utc(self, redis_mock):
        """Verify ErrorHandler records naive UTC timestamps."""
        redis_mock.lpush = AsyncMock()
        redis_mock.ltrim = AsyncMock()
        redis_mock.expire = AsyncMock()
        handler = ErrorHandler(redis_client=redis_mock)
        await handler.record_error(
            stream_id="test-stream",
            error_type="TestError",
            error_message="Test",
            is_recoverable=True,
        )
        redis_mock.lpush.assert_called_once()
        call_args = redis_mock.lpush.call_args
        error_str = call_args[0][1]
        payload = literal_eval(error_str)
        assert "timestamp" in payload
        assert payload["timestamp"].endswith("Z")

    def test_naive_utc_compatible_with_postgres_now(self):
        """Verify naive UTC is compatible with Postgres NOW()."""
        ts = _now_iso()
        parsed = datetime.fromisoformat(ts.rstrip("Z"))
        assert parsed.tzinfo is None
        ts_str = ts.rstrip("Z")
        assert "+" not in ts_str
        assert ts_str.count("-") == 2  # Only date separators


class TestTimestampFormat:
    """Test ISO8601 timestamp format compliance."""

    def test_iso8601_format(self):
        """Verify timestamps are valid ISO8601 format."""
        ts = _now_iso()
        parts = ts.rstrip("Z").split("T")
        assert len(parts) == 2
        date_part = parts[0]
        assert len(date_part) == 10  # YYYY-MM-DD
        time_part = parts[1]
        assert ":" in time_part

    def test_iso8601_roundtrip(self):
        """Verify ISO8601 timestamp can round-trip."""
        ts = _now_iso()
        parsed = datetime.fromisoformat(ts.rstrip("Z"))
        reparsed = parsed.isoformat() + "Z"
        parsed2 = datetime.fromisoformat(reparsed.rstrip("Z"))
        diff = (parsed - parsed2).total_seconds()
        assert abs(diff) < 0.001
