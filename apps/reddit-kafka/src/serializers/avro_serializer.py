"""Kafka message serialization using AWS Glue Schema Registry and Avro."""

import logging
from functools import lru_cache
from typing import cast

import boto3
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.adapter.kafka import KafkaSerializer
from aws_schema_registry.avro import AvroSchema
from botocore.exceptions import ClientError, NoCredentialsError
from pydantic import BaseModel, ValidationError

from config import SchemaSettings
from src.models import RedditCommentMessage

logger = logging.getLogger(__name__)


class AvroMessageSerializer[T: BaseModel]:
    """Serialize Kafka messages with Avro and AWS Glue Schema Registry."""

    def __init__(
        self,
        registry_name: str,
        schema_name: str,
        aws_region: str,
        topic_name: str,
        use_localstack: bool = False,
        localstack_url: str = "http://localhost:4566",
    ):
        self.schema_name = schema_name
        self.topic_name = topic_name

        if use_localstack:
            logger.info("Initializing Glue client for LocalStack")
            self.glue_client = boto3.client(
                "glue",
                region_name=aws_region,
                endpoint_url=localstack_url,
                aws_access_key_id="test",
                aws_secret_access_key="test",
            )
        else:
            logger.info("Initializing AWS Glue client in %s", aws_region)
            self.glue_client = boto3.client("glue", region_name=aws_region)

        try:
            schema_version = self.glue_client.get_schema_version(
                SchemaId={"RegistryName": registry_name, "SchemaName": schema_name},
                SchemaVersionNumber={"LatestVersion": True},
            )
            self.avro_schema = AvroSchema(schema_version["SchemaDefinition"])
            logger.info("Loaded Avro schema for %s from AWS Glue", schema_name)
        except ClientError as error:
            logger.error(
                "Failed to load schema %s/%s: %s", registry_name, schema_name, error
            )
            raise RuntimeError(
                "Cannot start serializer without schema access."
            ) from error

        try:
            self.registry_client = SchemaRegistryClient(
                self.glue_client,
                registry_name=registry_name,
            )
            self.serializer = KafkaSerializer(self.registry_client)
        except (NoCredentialsError, ClientError) as error:
            logger.error("Failed to initialize Schema Registry Client: %s", error)
            raise RuntimeError(
                "Cannot start serializer without Registry access."
            ) from error

    def serialize(self, message: T) -> bytes:
        """
        Serializes a Pydantic model to Avro bytes.
        Raises ValueError on validation failure, or generic
        exceptions on serialization failure.
        """
        try:
            message_dict = message.model_dump()
            return cast(
                bytes,
                self.serializer.serialize(
                    topic=self.topic_name,
                    value=(message_dict, self.avro_schema),
                ),
            )
        except ValidationError as error:
            logger.error(
                "Pydantic validation failed for %s:\n%s", self.schema_name, error
            )
            raise ValueError(
                f"Invalid message format for schema {self.schema_name}"
            ) from error
        except Exception as error:
            logger.error("AWS Glue serialization failed: %s", error)
            raise


@lru_cache(maxsize=4)
def _get_cached_serializer(
    registry_name: str,
    schema_name: str,
    aws_region: str,
    use_localstack: bool,
    topic_name: str,
) -> AvroMessageSerializer[RedditCommentMessage]:
    return AvroMessageSerializer(
        registry_name=registry_name,
        schema_name=schema_name,
        aws_region=aws_region,
        use_localstack=use_localstack,
        topic_name=topic_name,
    )


def get_serializer(
    schema_settings: SchemaSettings,
    topic_name: str,
) -> AvroMessageSerializer[RedditCommentMessage]:
    """Get or create the message serializer singleton."""
    return _get_cached_serializer(
        registry_name=schema_settings.registry_name,
        schema_name=schema_settings.schema_name,
        aws_region=schema_settings.aws_region,
        use_localstack=schema_settings.use_localstack,
        topic_name=topic_name,
    )


__all__ = ["AvroMessageSerializer", "get_serializer"]
