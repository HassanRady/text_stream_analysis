import logging
import uuid
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import sessionmaker

from src.config import Settings
from src.db import close_db, init_db, get_engine
from src.redis_client import close_redis, get_redis
from src.repositories.stream_registry import StreamExistsError, StreamRegistry
from src.stream.manager import StreamManager
from src.tasks import CheckpointFlusher

logger = logging.getLogger(__name__)


# Example runner for smoke testing: replace with your actual runner
async def example_runner(subreddit: str):
    # simulate a long-running stream
    while True:
        print("streaming:", subreddit)
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    settings = Settings()

    await init_db(settings.postgres)
    logger.info("✓ Postgres initialized")

    redis = get_redis(settings.redis)
    await redis.ping()
    logger.info("✓ Redis connected")

    app.state.registry = StreamRegistry(redis)
    app.state.instance_id = str(uuid.uuid4())[:8]
    app.state.manager = StreamManager(app.state.registry, example_runner, app.state.instance_id)
    logger.info(f"✓ Manager initialized (instance_id={app.state.instance_id})")

    engine = get_engine()
    if engine is None:
        raise RuntimeError("Engine not initialized after init_db")

    session_maker = sessionmaker(
        bind=engine,  # type: ignore
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    app.state.flusher = CheckpointFlusher(redis, session_maker, flush_interval=settings.db_flush_interval)
    await app.state.flusher.start()
    logger.info("✓ Checkpoint flusher started")

    yield

    logger.info("Shutting down...")

    if hasattr(app.state, "flusher"):
        await app.state.flusher.stop()
        logger.info("✓ Checkpoint flusher stopped")

        await app.state.manager.stop_all()
        logger.info("✓ All streams stopped")

    await close_redis()
    logger.info("✓ Redis closed")

    await close_db()
    logger.info("✓ Postgres closed")

    logger.info("Shutdown complete")


app = FastAPI(lifespan=lifespan)

@app.post("/streams")
async def create_stream(subreddit: str):
    try:
        meta = await app.state.manager.start_stream(subreddit)
        return meta
    except StreamExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))

@app.get("/streams")
async def list_streams():
    return await app.state.registry.list_streams()

@app.post("/streams/{stream_id}/stop")
async def stop_stream(stream_id: str):
    await app.state.manager.stop_stream(stream_id)
    return {"stream_id": stream_id, "status": "stopping"}