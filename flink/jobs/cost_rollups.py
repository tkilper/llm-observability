"""
cost_rollups.py — Flink SQL job

Computes tumbling window aggregations over llm.events.raw and writes
per-model / per-user cost and usage rollups to PostgreSQL.

Window size is configurable via ROLLUP_WINDOW_MINUTES (default: 60).
Set to 5 for demo runs so results appear quickly.
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
JDBC_URL = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:5432/{os.environ['POSTGRES_DB']}"
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
WINDOW_MINUTES = int(os.getenv("ROLLUP_WINDOW_MINUTES", "60"))

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
    prompt_tokens     INT,
    completion_tokens INT,
    total_tokens      INT,
    cost_usd          DOUBLE,
    latency_ms        INT,
    status            STRING,
    event_time AS TO_TIMESTAMP(LEFT(`timestamp`, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'llm.events.raw',
    'properties.bootstrap.servers'  = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'           = 'flink-cost-rollups',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
)
""")

# ── Sink: PostgreSQL cost_rollups_hourly ──────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE postgres_sink (
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    model           STRING,
    provider        STRING,
    user_id         STRING,
    total_requests  BIGINT,
    success_count   BIGINT,
    error_count     BIGINT,
    total_tokens    BIGINT,
    total_cost_usd  DOUBLE,
    avg_latency_ms  DOUBLE,
    PRIMARY KEY (window_start, model, user_id) NOT ENFORCED
) WITH (
    'connector'                      = 'jdbc',
    'url'                            = '{JDBC_URL}',
    'table-name'                     = 'cost_rollups_hourly',
    'username'                       = '{POSTGRES_USER}',
    'password'                       = '{POSTGRES_PASSWORD}',
    'driver'                         = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows'     = '100',
    'sink.buffer-flush.interval'     = '10s',
    'sink.max-retries'               = '3'
)
""")

# ── Pipeline: tumbling window aggregation ─────────────────────────────────────
t_env.execute_sql(f"""
INSERT INTO postgres_sink
SELECT
    TUMBLE_START(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE)  AS window_start,
    TUMBLE_END(event_time,   INTERVAL '{WINDOW_MINUTES}' MINUTE)  AS window_end,
    model,
    provider,
    user_id,
    COUNT(*)                                                        AS total_requests,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)            AS success_count,
    SUM(CASE WHEN status <> 'success' THEN 1 ELSE 0 END)           AS error_count,
    SUM(CAST(total_tokens AS BIGINT))                              AS total_tokens,
    SUM(cost_usd)                                                   AS total_cost_usd,
    AVG(CAST(latency_ms AS DOUBLE))                                AS avg_latency_ms
FROM kafka_source
GROUP BY
    TUMBLE(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE),
    model,
    provider,
    user_id
""").wait()
