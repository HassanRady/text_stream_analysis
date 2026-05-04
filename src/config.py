from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedditSettings(BaseSettings):
    client_id: str = Field(alias="REDDIT_CLIENT_ID")
    client_secret: SecretStr = Field(alias="REDDIT_CLIENT_SECRET")
    user_agent: str = Field(alias="REDDIT_USER_AGENT")
    # username: str = Field(alias="REDDIT_USERNAME")
    # password: SecretStr = Field(alias="REDDIT_PASSWORD")


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(alias="KAFKA_BOOTSTRAP_SERVERS")
    raw_text_topic: str = Field(alias="KAFKA_RAW_TEXT_TOPIC")


class RedisSettings(BaseSettings):
    host: str = Field(alias="REDIS_HOST")
    port: int = Field(alias="REDIS_PORT")
    user: str = Field(alias="REDIS_USER")
    password: SecretStr = Field(alias="REDIS_PASSWORD")


class PostgresSettings(BaseSettings):
    host: str = Field(alias="POSTGRES_HOST")
    port: int = Field(alias="POSTGRES_PORT")
    user: str = Field(alias="POSTGRES_USER")
    password: SecretStr = Field(alias="POSTGRES_PASSWORD")
    db: str = Field(alias="POSTGRES_DB")


class Settings(BaseSettings):
    reddit: RedditSettings = RedditSettings()
    # kafka: KafkaSettings = KafkaSettings()
    redis: RedisSettings = RedisSettings()
    postgres: PostgresSettings = PostgresSettings()
