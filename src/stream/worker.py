"""Cancel-safe stream worker that wraps the streaming loop."""

import asyncio
import logging
from datetime import datetime, timezone

import asyncpraw
from asyncprawcore.exceptions import RequestException, TooManyRequests, NotFound, Forbidden

from src.repositories.stream_registry import StreamRegistry
from src.stream.circuit_breaker import CircuitBreaker
from src.stream.error_handler import ErrorHandler, RecoveryStrategy
from src.stream.lock import DistributedLockManager

logger = logging.getLogger(__name__)


class StreamWorker:
    """
    Cancel-safe worker for streaming Reddit comments.

    Responsibilities:
    - Listen for CancelledError (graceful shutdown)
    - Use circuit breaker for rate limit handling
    - Track errors for debugging
    - Update checkpoints periodically
    - Support Ctrl+C and lifespan shutdown
    """

    def __init__(
        self,
        subreddit: str,
        reddit_client: asyncpraw.Reddit,
        kafka_producer,
        registry: StreamRegistry,
        lock_manager: DistributedLockManager,
        instance_id: str,
        stream_id: str,
        kafka_topic: str,
    ):
        """
        Args:
            subreddit: Subreddit name to stream
            reddit_client: PRAW Reddit client (async)
            kafka_producer: Kafka producer (confluent_kafka)
            registry: StreamRegistry for checkpoints
            lock_manager: DistributedLockManager for locks
            instance_id: Instance ID
            stream_id: Stream UUID
            kafka_topic: Kafka topic to produce to
        """
        self.subreddit = subreddit
        self.reddit_client = reddit_client
        self.kafka_producer = kafka_producer
        self.registry = registry
        self.lock_manager = lock_manager
        self.instance_id = instance_id
        self.stream_id = stream_id
        self.kafka_topic = kafka_topic

        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
        )
        # Pass through any session_maker from registry so ErrorHandler can persist errors
        self.error_handler = ErrorHandler(
            registry._redis, session_maker=getattr(registry, "_session_maker", None)
        )

        self.checkpoint_interval = 100
        self.comments_since_checkpoint = 0
        self.lock_refresh_interval = 30

    async def run(self) -> None:
        """Run the streaming loop (cancel-safe).

        This is the main entry point. Handles:
        - Cancel signals gracefully
        - Errors with exponential backoff
        - Checkpoint saves
        - Lock refresh
        """
        shutdown_event = False

        try:
            logger.info(f"StreamWorker starting for {self.subreddit}")

            lock_task = asyncio.create_task(self._lock_refresh_loop())

            try:
                while True:  # Main loop (broken by CancelledError)
                    try:
                        await self.circuit_breaker.call(
                            self._fetch_and_process_comments()
                        )
                    except asyncio.CancelledError:
                        logger.info(f"StreamWorker cancelled for {self.subreddit}")
                        shutdown_event = True
                        break
                    except Exception as e:
                        await self._handle_error(e)

            finally:
                lock_task.cancel()
                try:
                    await lock_task
                except asyncio.CancelledError:
                    pass

        finally:
            if shutdown_event:
                try:
                    await self._save_checkpoint()
                    logger.info(f"✓ Saved final checkpoint for {self.subreddit}")
                except Exception as e:
                    logger.error(f"Error saving final checkpoint: {e}")

            try:
                await self.lock_manager.release_lock(self.subreddit, self.instance_id)
                logger.info(f"✓ Released lock for {self.subreddit}")
            except Exception as e:
                logger.error(f"Error releasing lock: {e}")

            logger.info(f"StreamWorker finished for {self.subreddit}")

    async def _fetch_and_process_comments(self) -> None:
        """Fetch comments from Reddit and produce to Kafka.

        Called periodically by circuit breaker. Handles one batch of comments
        or one streaming session until backoff is needed.
        """
        subreddit = await self.reddit_client.subreddit(self.subreddit)

        checkpoint = await self.registry.get_checkpoint(self.stream_id)
        skip_existing = checkpoint.get("last_comment_id") is None

        try:
            async for comment in subreddit.stream.comments(skip_existing=skip_existing):
                # Check for cancellation
                await asyncio.sleep(0)  # Yield control to allow cancellation

                if comment is None:
                    continue

                if (
                    checkpoint.get("last_comment_id")
                    and comment.id == checkpoint["last_comment_id"]
                ):
                    logger.debug(f"Skipping already-processed comment {comment.id}")
                    continue

                try:
                    value = {
                        "subreddit": self.subreddit,
                        "author_id": comment.author.name
                        if comment.author
                        else "[deleted]",
                        "text": comment.body,
                        "timestamp": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
                    }

                    import json

                    self.kafka_producer.produce(
                        self.kafka_topic,
                        json.dumps(value).encode("utf-8"),
                    )
                    self.kafka_producer.flush()

                    # Update checkpoint every N comments
                    self.comments_since_checkpoint += 1
                    if self.comments_since_checkpoint >= self.checkpoint_interval:
                        await self._save_checkpoint_for_comment(comment)
                        self.comments_since_checkpoint = 0

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(f"Error processing comment {comment.id}: {e}")
                    await self.error_handler.record_error(
                        self.stream_id,
                        "CommentProcessingError",
                        str(e),
                        is_recoverable=True,
                    )

        except asyncio.CancelledError:
            raise
        except TooManyRequests as e:
            logger.error("Rate limited by Reddit API")
            await self.error_handler.record_error(
                self.stream_id,
                "TooManyRequests",
                str(e),
                is_recoverable=True,
            )
            raise
        except (NotFound, Forbidden) as e:
            logger.error("Subreddit unavailable: %s", e)
            await self.error_handler.record_error(
                self.stream_id,
                e.__class__.__name__,
                str(e),
                is_recoverable=False,
            )
            await self.registry.update_status(self.stream_id, "error")
            raise
        except RequestException as e:
            logger.error(f"Reddit API request error: {e}")
            await self.error_handler.record_error(
                self.stream_id,
                "RequestException",
                str(e),
                is_recoverable=True,
            )
            raise
        except Exception as e:
            logger.exception(f"Unexpected error in fetch_and_process: {e}")
            await self.error_handler.record_error(
                self.stream_id,
                e.__class__.__name__,
                str(e),
                is_recoverable=ErrorHandler.is_retryable(e),
            )
            raise

    async def _save_checkpoint_for_comment(self, comment) -> None:
        """Save checkpoint after processing a comment."""
        try:
            await self.registry.set_checkpoint(
                self.stream_id,
                last_comment_id=comment.id,
                last_processed_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            )
            logger.debug(f"Checkpoint saved: {comment.id}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    async def _save_checkpoint(self) -> None:
        """Save current checkpoint (called on shutdown)."""
        checkpoint = await self.registry.get_checkpoint(self.stream_id)
        if checkpoint.get("last_comment_id"):
            await self._save_checkpoint_for_comment(
                type("", (), {"id": checkpoint["last_comment_id"]})()
            )

    async def _lock_refresh_loop(self) -> None:
        """Periodically refresh the distributed lock."""
        try:
            while True:
                await asyncio.sleep(self.lock_refresh_interval)
                success = await self.lock_manager.refresh_lock(
                    self.subreddit,
                    self.instance_id,
                    ttl=60,
                )
                if not success:
                    logger.error(f"Failed to refresh lock for {self.subreddit}")
        except asyncio.CancelledError:
            pass

    async def _handle_error(self, error: Exception) -> None:
        """Handle error and decide on recovery strategy."""
        error_type = error.__class__.__name__

        await self.error_handler.record_error(
            self.stream_id,
            error_type,
            str(error),
            is_recoverable=ErrorHandler.is_retryable(error),
        )

        if RecoveryStrategy.should_abandon_stream(error):
            logger.error(f"Fatal error, abandoning stream: {error}")
            await self.registry.update_status(self.stream_id, "error")
            raise error

        if RecoveryStrategy.should_retry_with_backoff(error):
            backoff = ErrorHandler.get_backoff_duration(error)
            logger.warning(f"Rate limited, backing off for {backoff}s")
            await asyncio.sleep(backoff)

        elif RecoveryStrategy.should_retry_immediately(error):
            logger.warning(f"Retryable error, retrying immediately: {error}")

        else:
            logger.error(f"Unhandled error: {error}")
            raise error
