"""StreamManager: starts/stops per-subreddit streaming tasks and coordinates with StreamRegistry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from src.repositories.stream_registry import StreamRegistry, StreamNotFoundError

logger = logging.getLogger(__name__)


class StreamManager:
    """Manage lifecycle of streams on the local instance.

    A StreamManager does not itself implement the streaming loop; instead it
    accepts a `runner` callable (subreddit -> awaitable) which performs the
    actual work for a stream. This keeps the manager testable and decoupled.

    Example runner signature: async def runner(subreddit: str): ...
    """

    def __init__(
        self,
        registry: StreamRegistry,
        runner: Callable[[str], Awaitable[None]],
        instance_id: str,
    ):
        self.registry = registry
        self.runner = runner
        self.instance_id = instance_id
        # local map of stream_id -> asyncio.Task
        self._tasks: Dict[str, asyncio.Task] = {}
        # protects _tasks
        self._lock = asyncio.Lock()

    async def start_stream(
        self, subreddit: str, config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        meta = await self.registry.create_stream(
            subreddit, config=config, instance_id=self.instance_id
        )
        stream_id = meta["id"]

        async with self._lock:
            if stream_id in self._tasks:
                logger.warning("stream %s already running locally", stream_id)
                return meta

            task = asyncio.create_task(
                self._run(stream_id, subreddit), name=f"stream-{stream_id}"
            )
            self._tasks[stream_id] = task
        await self.registry.update_status(
            stream_id, "active", instance_id=self.instance_id
        )
        return meta

    async def _run(self, stream_id: str, subreddit: str) -> None:
        """Wrapper around the user-provided runner. Handles lifecycle updates and errors."""
        try:
            logger.info("starting runner for %s (id=%s)", subreddit, stream_id)
            await self.runner(subreddit)
            # runner returned normally - mark stopped
            await self.registry.update_status(stream_id, "stopped")
            logger.info("runner finished for %s (id=%s)", subreddit, stream_id)
        except asyncio.CancelledError:
            # graceful cancellation
            await self.registry.update_status(stream_id, "stopped")
            logger.info("runner cancelled for %s (id=%s)", subreddit, stream_id)
            raise
        except StreamNotFoundError:
            await self.registry.update_status(stream_id, "error")
            logger.exception("stream not found in registry during run: %s", stream_id)
        except Exception:
            await self.registry.update_status(stream_id, "error")
            logger.exception("stream %s crashed", stream_id)
        finally:
            async with self._lock:
                if stream_id in self._tasks:
                    del self._tasks[stream_id]

    async def stop_stream(self, stream_id: str) -> None:
        async with self._lock:
            task = self._tasks.get(stream_id)
            if not task:
                logger.info("stop requested for non-local stream %s", stream_id)
                try:
                    await self.registry.update_status(stream_id, "stopped")
                except Exception:
                    pass
                return
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def list_local_streams(self) -> Dict[str, str]:
        """Return a mapping of local stream_id -> task_name for streams running on this instance."""
        async with self._lock:
            return {sid: t.get_name() for sid, t in self._tasks.items()}

    async def is_running_locally(self, stream_id: str) -> bool:
        async with self._lock:
            return stream_id in self._tasks

    async def stop_all(self) -> None:
        async with self._lock:
            stream_ids = list(self._tasks.keys())

        for stream_id in stream_ids:
            try:
                await self.stop_stream(stream_id)
            except Exception:
                logger.exception("error stopping stream %s", stream_id)

        logger.info("all streams stopped")

