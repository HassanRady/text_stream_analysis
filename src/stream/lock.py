"""Distributed lock manager for preventing duplicate streams across instances."""

import logging
import uuid

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class DistributedLockManager:
    """
    Manages distributed locks using Redis with TTL.

    Prevents multiple instances from starting the same stream (identified by subreddit).
    Uses SET NX EX pattern for atomic lock acquisition and TTL.
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Args:
            redis_client: Async Redis client
        """
        self.redis = redis_client

    def _lock_key(self, subreddit: str) -> str:
        """Generate Redis key for lock."""
        return f"stream:lock:{subreddit}"

    async def acquire_lock(
        self,
        subreddit: str,
        instance_id: str,
        ttl: int = 60,
    ) -> bool:
        """Acquire a distributed lock for a subreddit.

        Args:
            subreddit: Subreddit name (used as lock identifier)
            instance_id: Unique identifier of this instance
            ttl: Lock TTL in seconds (default 60s)

        Returns:
            True if lock acquired, False if already held
        """
        key = self._lock_key(subreddit)
        token = str(uuid.uuid4())

        acquired = await self.redis.set(
            key,
            f"{instance_id}:{token}",
            nx=True,
            ex=ttl,
        )

        if acquired:
            logger.debug(f"✓ Acquired lock for {subreddit} (instance={instance_id})")
            return True
        else:
            holder = await self.redis.get(key)
            logger.warning(
                f"✗ Lock already held for {subreddit} by {holder or 'unknown'}"
            )
            return False

    async def refresh_lock(
        self,
        subreddit: str,
        instance_id: str,
        ttl: int = 60,
    ) -> bool:
        """Refresh an existing lock (extend TTL).

        Args:
            subreddit: Subreddit name
            instance_id: Instance ID that holds the lock
            ttl: New TTL in seconds

        Returns:
            True if refreshed successfully, False if lock token doesn't match
        """
        key = self._lock_key(subreddit)
        current = await self.redis.get(key)

        if not current:
            logger.warning(f"✗ Lock expired for {subreddit}")
            return False

        if not current.startswith(instance_id):
            logger.warning(
                f"✗ Cannot refresh lock for {subreddit}: held by {current}, "
                f"not {instance_id}"
            )
            return False

        success = await self.redis.expire(key, ttl)
        if success:
            logger.debug(f"✓ Refreshed lock for {subreddit}")
        return success > 0

    async def release_lock(self, subreddit: str, instance_id: str) -> bool:
        """Release a lock (delete from Redis).

        Args:
            subreddit: Subreddit name
            instance_id: Instance ID that holds the lock

        Returns:
            True if released, False if not held by this instance
        """
        key = self._lock_key(subreddit)
        current = await self.redis.get(key)

        if not current or not current.startswith(instance_id):
            logger.warning(
                f"✗ Cannot release lock for {subreddit}: not held by {instance_id}"
            )
            return False

        deleted = await self.redis.delete(key)
        if deleted:
            logger.debug(f"✓ Released lock for {subreddit}")
        return deleted > 0

    async def is_locked(self, subreddit: str) -> bool:
        """Check if a lock currently exists.

        Args:
            subreddit: Subreddit name

        Returns:
            True if locked, False otherwise
        """
        key = self._lock_key(subreddit)
        exists = await self.redis.exists(key)
        return exists > 0

    async def get_lock_holder(self, subreddit: str) -> str | None:
        """Get the current lock holder.

        Args:
            subreddit: Subreddit name

        Returns:
            Lock holder info (format: instance_id:token), or None if not locked
        """
        key = self._lock_key(subreddit)
        return await self.redis.get(key)
