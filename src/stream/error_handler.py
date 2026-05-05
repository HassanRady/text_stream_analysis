import logging
import uuid
from datetime import UTC, datetime

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class ErrorHandler:
    """
    Tracks errors and determines recovery strategies.

    Stores errors in Redis for short-term tracking (configurable TTL).
    Logs errors to help debugging.
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Args:
            redis_client: Async Redis client
        """
        self.redis = redis_client
        self.error_ttl = 86400  # 24 hours

    def _error_key(self, stream_id: str) -> str:
        """Generate Redis key for error tracking."""
        return f"stream:errors:{stream_id}"

    async def record_error(
        self,
        stream_id: str,
        error_type: str,
        error_message: str,
        is_recoverable: bool = True,
    ) -> None:
        """Record an error in Redis.

        Args:
            stream_id: Stream ID
            error_type: Exception class name (e.g., 'TooManyRequests')
            error_message: Exception message
            is_recoverable: Whether this error is recoverable (affects backoff)
        """
        key = self._error_key(stream_id)

        error_entry = {
            "id": str(uuid.uuid4()),
            "error_type": error_type,
            "error_message": error_message,
            "is_recoverable": "1" if is_recoverable else "0",
            "timestamp": datetime.now(UTC).isoformat() + "Z",
        }

        # Push to Redis list (keep last 100 errors)
        await self.redis.lpush(key, str(error_entry))  # Add to front
        await self.redis.ltrim(key, 0, 99)  # Keep last 100
        await self.redis.expire(key, self.error_ttl)  # Auto-expire after 24h

        logger.warning(
            f"Stream {stream_id}: {error_type} - {error_message} "
            f"(recoverable={is_recoverable})"
        )

    async def get_error_count(self, stream_id: str) -> int:
        """Get number of recent errors for a stream.

        Args:
            stream_id: Stream ID

        Returns:
            Number of errors in the last 24 hours
        """
        key = self._error_key(stream_id)
        return await self.redis.llen(key)

    async def clear_errors(self, stream_id: str) -> None:
        """Clear error history for a stream.

        Args:
            stream_id: Stream ID
        """
        key = self._error_key(stream_id)
        await self.redis.delete(key)
        logger.debug(f"Cleared error history for stream {stream_id}")

    async def get_recent_errors(self, stream_id: str, limit: int = 10) -> list:
        """Get recent errors for a stream.

        Args:
            stream_id: Stream ID
            limit: Number of recent errors to return

        Returns:
            List of error entries
        """
        key = self._error_key(stream_id)
        error_strings = await self.redis.lrange(key, 0, limit - 1)
        return [eval(e) for e in error_strings if e]  # Convert string back to dict

    @staticmethod
    def is_retryable(error: Exception) -> bool:
        """Check if an error is transient and retryable.

        Args:
            error: Exception instance

        Returns:
            True if retryable, False if fatal
        """
        # Retryable errors (transient)
        retryable_types = [
            "ConnectionError",
            "TimeoutError",
            "TooManyRequests",
            "RequestException",
            "SuspiciousActivity",  # Reddit temporary ban
            "ResponseException",
        ]

        error_type = error.__class__.__name__
        return error_type in retryable_types

    @staticmethod
    def should_backoff(error: Exception) -> bool:
        """Check if error requires backoff (rate limit, not generic error).

        Args:
            error: Exception instance

        Returns:
            True if backoff is needed
        """
        backoff_types = ["TooManyRequests", "RateLimitError"]
        return error.__class__.__name__ in backoff_types

    @staticmethod
    def get_backoff_duration(error: Exception) -> int:
        """Get recommended backoff duration for error.

        Args:
            error: Exception instance

        Returns:
            Recommended backoff in seconds
        """
        error_type = error.__class__.__name__

        # Rate limit errors require aggressive backoff
        if error_type == "TooManyRequests":
            return 60  # Start with 60s, exponential backoff will increase
        elif error_type == "RateLimitError":
            return 30
        else:
            return 5  # Default short backoff for network errors


class RecoveryStrategy:
    """
    Determines recovery action based on error type and context.
    """

    @staticmethod
    def should_retry_immediately(error: Exception) -> bool:
        """Determine if error warrants immediate retry."""
        retryable = ErrorHandler.is_retryable(error)
        is_rate_limit = ErrorHandler.should_backoff(error)
        return retryable and not is_rate_limit

    @staticmethod
    def should_retry_with_backoff(error: Exception) -> bool:
        """Determine if error warrants retry with backoff."""
        return ErrorHandler.should_backoff(error)

    @staticmethod
    def should_abandon_stream(error: Exception) -> bool:
        """Determine if error is fatal and stream should be abandoned."""
        non_retryable = [
            "NotFound",
            "BadRequest",
            "Forbidden",
            "InvalidCredentials",
            "ValueError",
        ]
        return error.__class__.__name__ in non_retryable
