"""
event_sink.py — Flink Table API job

Reads raw LLM call events from Kafka (llm.events.raw) and writes them
to the llm_events table in PostgreSQL with exactly-once semantics via
UPSERT on event_id.
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
JDBC_URL = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:5432/{os.environ['POSTGRES_DB']}"
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
t_env = StreamTableEnvironment.create(env)

# ── Source: Kafka llm.events.raw ──────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE kafka_source (
    event_id          STRING,
    `timestamp`       STRING,
    model             STRING,
    provider          STRING,
    user_id           STRING,
    session_id        STRING,
    prompt_tokens     INT,
    completion_tokens INT,
    total_tokens      INT,
    cost_usd          DOUBLE,
    latency_ms        INT,
    status            STRING,
    error_code        STRING,
    request_id        STRING,
    -- Parse ISO-8601 timestamp; producer always emits 'yyyy-MM-ddTHH:mm:ss.SSSSSS+00:00'
    event_time AS TO_TIMESTAMP(LEFT(`timestamp`, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'llm.events.raw',
    'properties.bootstrap.servers'  = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'           = 'flink-event-sink',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
)
""")

# ── Sink: PostgreSQL llm_events ───────────────────────────────────────────────
# PRIMARY KEY triggers UPSERT mode — safe to replay without duplicates
t_env.execute_sql(f"""
CREATE TABLE postgres_sink (
    event_id          STRING,
    ts                TIMESTAMP(3),
    model             STRING,
    provider          STRING,
    user_id           STRING,
    session_id        STRING,
    prompt_tokens     INT,
    completion_tokens INT,
    total_tokens      INT,
    cost_usd          DOUBLE,
    latency_ms        INT,
    status            STRING,
    error_code        STRING,
    request_id        STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector'                      = 'jdbc',
    'url'                            = '{JDBC_URL}',
    'table-name'                     = 'llm_events',
    'username'                       = '{POSTGRES_USER}',
    'password'                       = '{POSTGRES_PASSWORD}',
    'driver'                         = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows'     = '500',
    'sink.buffer-flush.interval'     = '2s',
    'sink.max-retries'               = '3'
)
""")

# ── Pipeline ──────────────────────────────────────────────────────────────────
t_env.execute_sql("""
INSERT INTO postgres_sink
SELECT
    event_id,
    event_time          AS ts,
    model,
    provider,
    user_id,
    session_id,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    cost_usd,
    latency_ms,
    status,
    error_code,
    request_id
FROM kafka_source
""").wait()
