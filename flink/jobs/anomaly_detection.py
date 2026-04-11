"""
anomaly_detection.py — Flink SQL job

Applies 5-minute tumbling windows over llm.events.raw, computes per-model
window statistics, and emits anomaly events to:
  - PostgreSQL: anomaly_events table (consumed by Grafana)
  - Kafka: llm.events.anomalies topic (available for downstream consumers)

Anomaly types detected:
  latency_spike  — avg window latency exceeds warning (3 s) or critical (6 s)
  error_surge    — window error rate exceeds warning (15%) or critical (40%)
  cost_spike     — window total cost exceeds warning ($1) or critical ($5)
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
JDBC_URL = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:5432/{os.environ['POSTGRES_DB']}"
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
WINDOW_MINUTES = int(os.getenv("ANOMALY_WINDOW_MINUTES", "5"))

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
t_env = StreamTableEnvironment.create(env)

# ── Source: Kafka llm.events.raw ──────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE kafka_source (
    `timestamp`   STRING,
    model         STRING,
    cost_usd      DOUBLE,
    latency_ms    INT,
    status        STRING,
    event_time AS TO_TIMESTAMP(LEFT(`timestamp`, 26), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'llm.events.raw',
    'properties.bootstrap.servers'  = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'           = 'flink-anomaly-detector',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
)
""")

# ── Intermediate view: per-model window statistics ────────────────────────────
t_env.execute_sql(f"""
CREATE TEMPORARY VIEW window_stats AS
SELECT
    TUMBLE_START(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE)   AS window_start,
    TUMBLE_END(event_time,   INTERVAL '{WINDOW_MINUTES}' MINUTE)   AS window_end,
    model,
    AVG(CAST(latency_ms AS DOUBLE))                                AS avg_latency_ms,
    CAST(
        SUM(CASE WHEN status <> 'success' THEN 1 ELSE 0 END) AS DOUBLE
    ) / COUNT(*)                                                    AS error_rate,
    SUM(cost_usd)                                                   AS total_cost_usd
FROM kafka_source
GROUP BY
    TUMBLE(event_time, INTERVAL '{WINDOW_MINUTES}' MINUTE),
    model
""")

# ── Intermediate view: all flagged anomalies ──────────────────────────────────
t_env.execute_sql("""
CREATE TEMPORARY VIEW anomaly_stream AS

-- Latency spikes
SELECT
    CURRENT_TIMESTAMP                                                   AS detected_at,
    'latency_spike'                                                     AS anomaly_type,
    model,
    avg_latency_ms                                                      AS metric_value,
    CASE WHEN avg_latency_ms >= 6000.0 THEN 6000.0 ELSE 3000.0 END     AS threshold_value,
    CASE WHEN avg_latency_ms >= 6000.0 THEN 'critical' ELSE 'warning' END AS severity,
    window_start,
    window_end
FROM window_stats
WHERE avg_latency_ms >= 3000.0

UNION ALL

-- Error surges
SELECT
    CURRENT_TIMESTAMP                                                   AS detected_at,
    'error_surge'                                                       AS anomaly_type,
    model,
    error_rate                                                          AS metric_value,
    CASE WHEN error_rate >= 0.40 THEN 0.40 ELSE 0.15 END               AS threshold_value,
    CASE WHEN error_rate >= 0.40 THEN 'critical' ELSE 'warning' END     AS severity,
    window_start,
    window_end
FROM window_stats
WHERE error_rate >= 0.15

UNION ALL

-- Cost spikes
SELECT
    CURRENT_TIMESTAMP                                                   AS detected_at,
    'cost_spike'                                                        AS anomaly_type,
    model,
    total_cost_usd                                                      AS metric_value,
    CASE WHEN total_cost_usd >= 5.0 THEN 5.0 ELSE 1.0 END              AS threshold_value,
    CASE WHEN total_cost_usd >= 5.0 THEN 'critical' ELSE 'warning' END  AS severity,
    window_start,
    window_end
FROM window_stats
WHERE total_cost_usd >= 1.0
""")

# ── Sink 1: PostgreSQL anomaly_events ─────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE postgres_sink (
    detected_at     TIMESTAMP(3),
    anomaly_type    STRING,
    model           STRING,
    metric_value    DOUBLE,
    threshold_value DOUBLE,
    severity        STRING,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3)
) WITH (
    'connector'                  = 'jdbc',
    'url'                        = '{JDBC_URL}',
    'table-name'                 = 'anomaly_events',
    'username'                   = '{POSTGRES_USER}',
    'password'                   = '{POSTGRES_PASSWORD}',
    'driver'                     = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '50',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries'           = '3'
)
""")

# ── Sink 2: Kafka llm.events.anomalies ────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE kafka_sink (
    detected_at     TIMESTAMP(3),
    anomaly_type    STRING,
    model           STRING,
    metric_value    DOUBLE,
    threshold_value DOUBLE,
    severity        STRING,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'llm.events.anomalies',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'format'                       = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
""")

# ── Execute both sinks in a single job (shared source computation) ────────────
statement_set = t_env.create_statement_set()
statement_set.add_insert_sql("INSERT INTO postgres_sink SELECT * FROM anomaly_stream")
statement_set.add_insert_sql("INSERT INTO kafka_sink    SELECT * FROM anomaly_stream")
statement_set.execute().wait()
