import asyncio
from pathlib import Path

from sqlalchemy import text

from src.config import PostgresSettings
from src.db import build_postgres_url, init_db


async def run_migrations(settings: PostgresSettings) -> None:
    """Run all SQL migration files in migrations/ directory."""
    await init_db(settings)

    from src.db import get_engine

    engine = get_engine()
    if engine is None:
        raise RuntimeError("Engine not initialized")

    migrations_dir = Path(__file__).parent.parent / "migrations"
    migration_files = sorted(migrations_dir.glob("*.sql"))

    async with engine.begin() as conn:
        for migration_file in migration_files:
            print(f"Running migration: {migration_file.name}")
            sql = migration_file.read_text()
            await conn.execute(text(sql))
            print(f"✓ {migration_file.name} completed")


async def create_tables_from_models(settings: PostgresSettings) -> None:
    """Alternative: create tables from ORM models (requires Base.metadata)."""
    await init_db(settings)

    from src.db import get_engine
    from src.models import Base

    engine = get_engine()
    if engine is None:
        raise RuntimeError("Engine not initialized")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        print("✓ All tables created from models")


if __name__ == "__main__":
    settings = PostgresSettings()
    asyncio.run(run_migrations(settings))

