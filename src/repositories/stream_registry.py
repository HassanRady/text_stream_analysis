from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime

from src.redis_client import get_redis


class StreamExistsError(Exception):
    pass


class StreamNotFoundError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now().isoformat() + "Z"


class StreamRegistry:
    """Small helper to manage stream metadata/checkpoints in Redis.

    Key layout used here:
      - stream:meta:{stream_id} -> hash with metadata (subreddit,status,instance_id,config...)
      - stream:subreddit:{subreddit} -> stream_id (to detect duplicates)
      - stream:checkpoint:{stream_id} -> hash (last_comment_id, last_processed_at)
    """

    def __init__(self, redis=None):
        self._redis = redis

    @staticmethod
    def _meta_key(stream_id: str) -> str:
        return f"stream:meta:{stream_id}"

    @staticmethod
    def _subreddit_key(subreddit: str) -> str:
        return f"stream:subreddit:{subreddit}"

    @staticmethod
    def _checkpoint_key(stream_id: str) -> str:
        return f"stream:checkpoint:{stream_id}"

    async def create_stream(
        self,
        subreddit: str,
        config: Optional[Dict[str, Any]] = None,
        instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new stream if not exists. Raises StreamExistsError if a stream for the
        subreddit already exists.
        Returns the created stream metadata dict.
        """
        config = config or {}
        redis = self._redis
        sub_key = self._subreddit_key(subreddit)

        stream_id = str(uuid.uuid4())
        claimed = await redis.set(sub_key, stream_id, nx=True)
        if not claimed:
            # someone else already has a stream for this subreddit
            existing = await redis.get(sub_key)
            raise StreamExistsError(
                f"stream already exists for subreddit={subreddit} (id={existing})"
            )

        meta = {
            "id": stream_id,
            "subreddit": subreddit,
            "status": "starting",
            "instance_id": instance_id or "",
            "config": json.dumps(config),
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        await redis.hset(self._meta_key(stream_id), mapping=meta)
        return meta

    async def get_stream(self, stream_id: str) -> Dict[str, Any]:
        data = await self._redis.hgetall(self._meta_key(stream_id))
        if not data:
            raise StreamNotFoundError(stream_id)
        if "config" in data and data["config"]:
            try:
                data["config"] = json.loads(data["config"])
            except Exception:
                data["config"] = {}
        return data

    async def get_stream_by_subreddit(self, subreddit: str) -> Optional[Dict[str, Any]]:
        sid = await self._redis.get(self._subreddit_key(subreddit))
        if not sid:
            return None
        return await self.get_stream(sid)

    async def list_streams(self) -> List[Dict[str, Any]]:
        redis = self._redis
        cursor = 0
        results: List[Dict[str, Any]] = []
        pattern = "stream:meta:*"
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                pipe = redis.pipeline()
                for k in keys:
                    pipe.hgetall(k)
                rows = await pipe.execute()
                for data in rows:
                    if data:
                        if "config" in data and data["config"]:
                            try:
                                data["config"] = json.loads(data["config"])
                            except Exception:
                                data["config"] = {}
                        results.append(data)
            if cursor == 0:
                break
        return results

    async def update_status(
        self, stream_id: str, status: str, instance_id: Optional[str] = None
    ) -> None:
        mapping = {"status": status, "updated_at": _now_iso()}
        if instance_id is not None:
            mapping["instance_id"] = instance_id
        await self._redis.hset(self._meta_key(stream_id), mapping=mapping)

    async def delete_stream(self, stream_id: str) -> None:
        meta = await self._redis.hgetall(self._meta_key(stream_id))
        if not meta:
            return
        subreddit = meta.get("subreddit")
        pipe = self._redis.pipeline()
        pipe.delete(self._meta_key(stream_id))
        pipe.delete(self._checkpoint_key(stream_id))
        if subreddit:
            pipe.delete(self._subreddit_key(subreddit))
        await pipe.execute()

    async def set_checkpoint(
        self,
        stream_id: str,
        last_comment_id: Optional[str] = None,
        last_processed_at: Optional[str] = None,
    ) -> None:
        key = self._checkpoint_key(stream_id)
        mapping: Dict[str, str] = {}
        if last_comment_id is not None:
            mapping["last_comment_id"] = last_comment_id
        if last_processed_at is not None:
            mapping["last_processed_at"] = last_processed_at
        if mapping:
            mapping["updated_at"] = _now_iso()
            await self._redis.hset(key, mapping=mapping)

    async def get_checkpoint(self, stream_id: str) -> Dict[str, Optional[str]]:
        data = await self._redis.hgetall(self._checkpoint_key(stream_id))
        return {
            "last_comment_id": data.get("last_comment_id"),
            "last_processed_at": data.get("last_processed_at"),
            "updated_at": data.get("updated_at"),
        }
