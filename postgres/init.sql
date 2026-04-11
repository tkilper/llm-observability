-- =============================================================================
-- LLM Observability Schema
-- =============================================================================

-- Raw LLM call events streamed from Kafka via Flink
CREATE TABLE IF NOT EXISTS llm_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID            NOT NULL UNIQUE,
    ts              TIMESTAMPTZ     NOT NULL,
    model           VARCHAR(100)    NOT NULL,
    provider        VARCHAR(50)     NOT NULL,
    user_id         VARCHAR(100)    NOT NULL,
    session_id      VARCHAR(100)    NOT NULL,
    prompt_tokens   INTEGER         NOT NULL,
    completion_tokens INTEGER       NOT NULL,
    total_tokens    INTEGER         NOT NULL,
    cost_usd        NUMERIC(12, 6)  NOT NULL DEFAULT 0,
    latency_ms      INTEGER         NOT NULL,
    status          VARCHAR(20)     NOT NULL,  -- success | error | timeout
    error_code      VARCHAR(100),
    request_id      UUID            NOT NULL
);

-- Hourly cost + usage rollups per model and user (written by Flink)
CREATE TABLE IF NOT EXISTS cost_rollups_hourly (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    model           VARCHAR(100)    NOT NULL,
    provider        VARCHAR(50)     NOT NULL,
    user_id         VARCHAR(100)    NOT NULL,
    total_requests  INTEGER         NOT NULL DEFAULT 0,
    success_count   INTEGER         NOT NULL DEFAULT 0,
    error_count     INTEGER         NOT NULL DEFAULT 0,
    total_tokens    BIGINT          NOT NULL DEFAULT 0,
    total_cost_usd  NUMERIC(12, 4)  NOT NULL DEFAULT 0,
    avg_latency_ms  NUMERIC(10, 2),
    p95_latency_ms  NUMERIC(10, 2),
    UNIQUE (window_start, model, user_id)
);

-- Anomaly events detected by Flink (latency spikes, error surges, cost spikes)
CREATE TABLE IF NOT EXISTS anomaly_events (
    id              BIGSERIAL PRIMARY KEY,
    detected_at     TIMESTAMPTZ     NOT NULL,
    anomaly_type    VARCHAR(50)     NOT NULL,  -- latency_spike | error_surge | cost_spike
    model           VARCHAR(100),
    user_id         VARCHAR(100),
    metric_value    NUMERIC(12, 4)  NOT NULL,
    threshold_value NUMERIC(12, 4)  NOT NULL,
    severity        VARCHAR(20)     NOT NULL,  -- warning | critical
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL
);

-- =============================================================================
-- Indexes (tuned for Grafana time-series queries)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_llm_events_ts          ON llm_events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_llm_events_model_ts    ON llm_events (model, ts DESC);
CREATE INDEX IF NOT EXISTS idx_llm_events_user_ts     ON llm_events (user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_llm_events_status      ON llm_events (status, ts DESC);

CREATE INDEX IF NOT EXISTS idx_cost_rollups_window    ON cost_rollups_hourly (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_cost_rollups_model     ON cost_rollups_hourly (model, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_cost_rollups_user      ON cost_rollups_hourly (user_id, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_detected       ON anomaly_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_type           ON anomaly_events (anomaly_type, detected_at DESC);

-- =============================================================================
-- Grafana read-only user
-- =============================================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'grafana_readonly';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE llm_observability TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
