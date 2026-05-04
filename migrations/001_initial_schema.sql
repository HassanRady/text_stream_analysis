"""
001_initial_schema.sql

Creates initial tables:
- streams (stream registry)
- stream_checkpoints (recovery checkpoints)
- stream_errors (error tracking)
"""

CREATE TABLE IF NOT EXISTS streams (
    id VARCHAR(36) PRIMARY KEY,
    subreddit VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'inactive',
    instance_id VARCHAR(255),
    config JSONB DEFAULT '{}',
    last_heartbeat TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_streams_subreddit ON streams(subreddit);
CREATE INDEX IF NOT EXISTS idx_streams_status ON streams(status);
CREATE INDEX IF NOT EXISTS idx_streams_instance_id ON streams(instance_id);


CREATE TABLE IF NOT EXISTS stream_checkpoints (
    id VARCHAR(36) PRIMARY KEY,
    stream_id VARCHAR(36) UNIQUE NOT NULL,
    last_comment_id VARCHAR(255),
    last_processed_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_checkpoints_stream_id ON stream_checkpoints(stream_id);


CREATE TABLE IF NOT EXISTS stream_errors (
    id VARCHAR(36) PRIMARY KEY,
    stream_id VARCHAR(36) NOT NULL,
    error_type VARCHAR(100),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    is_recoverable INTEGER DEFAULT 1,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_errors_stream_id ON stream_errors(stream_id);

