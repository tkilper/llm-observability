"""
LLM Observability Dashboard
Streaming pipeline: Kafka -> Flink -> PostgreSQL -> Streamlit
"""

import os

import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="LLM Observability",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Auto-refresh every 10 seconds
st_autorefresh(interval=10_000, key="autorefresh")


# ── Database ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    host = os.getenv("POSTGRES_HOST", "postgres")
    db = os.getenv("POSTGRES_DB", "llm_observability")
    user = os.getenv("POSTGRES_USER", "grafana_reader")
    pw = os.getenv("POSTGRES_PASSWORD", "grafana_readonly")
    port = os.getenv("POSTGRES_PORT", "5432")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


@st.cache_data(ttl=10)
def q(sql: str) -> pd.DataFrame:
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Controls")
    st.markdown("**Time range**")
    time_range = st.selectbox(
        "Time range",
        options=["15 minutes", "30 minutes", "1 hour", "3 hours", "6 hours", "24 hours"],
        index=2,
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption("Auto-refreshes every 10 s")
    st.caption("Stack: Kafka → Flink → PostgreSQL")

interval = time_range

# ── Header ────────────────────────────────────────────────────────────────────
st.title("LLM Observability")
st.caption(f"Real-time monitoring · Last **{interval}**")

st.divider()

# ── KPI Metrics ───────────────────────────────────────────────────────────────
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

_requests = q(f"SELECT COUNT(*) AS cnt FROM llm_events WHERE ts >= NOW() - INTERVAL '{interval}'")
_cost = q(
    f"SELECT COALESCE(ROUND(SUM(cost_usd)::numeric, 4), 0) AS cost FROM llm_events WHERE ts >= NOW() - INTERVAL '{interval}'"
)
_latency = q(
    f"SELECT COALESCE(ROUND(AVG(latency_ms)::numeric, 0), 0) AS lat FROM llm_events WHERE ts >= NOW() - INTERVAL '{interval}' AND status = 'success'"
)
_anomalies = q(
    f"SELECT COUNT(*) AS cnt FROM anomaly_events WHERE detected_at >= NOW() - INTERVAL '{interval}'"
)

total_requests = int(_requests["cnt"].iloc[0]) if not _requests.empty else 0
total_cost = float(_cost["cost"].iloc[0]) if not _cost.empty else 0.0
avg_latency = int(_latency["lat"].iloc[0]) if not _latency.empty else 0
active_anomalies = int(_anomalies["cnt"].iloc[0]) if not _anomalies.empty else 0

kpi1.metric("Total Requests", f"{total_requests:,}")
kpi2.metric("Total Cost (USD)", f"${total_cost:.4f}")
kpi3.metric("Avg Latency (success)", f"{avg_latency:,} ms")
kpi4.metric("Active Anomalies", active_anomalies)

st.divider()

# ── Row 1: Event Rate + Error Rate ────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Event Rate by Model")
    df = q(f"""
        SELECT date_trunc('minute', ts) AS time, model, COUNT(*) AS events
        FROM llm_events
        WHERE ts >= NOW() - INTERVAL '{interval}'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.line(
            df,
            x="time",
            y="events",
            color="model",
            labels={"time": "", "events": "Events / min", "model": "Model"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for events...")

with col2:
    st.subheader("Error Rate by Model (%)")
    df = q(f"""
        SELECT
            date_trunc('minute', ts) AS time,
            model,
            ROUND(
                100.0 * SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS error_rate
        FROM llm_events
        WHERE ts >= NOW() - INTERVAL '{interval}'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.line(
            df,
            x="time",
            y="error_rate",
            color="model",
            labels={"time": "", "error_rate": "Error Rate (%)", "model": "Model"},
        )
        fig.add_hline(y=15, line_dash="dash", line_color="orange", annotation_text="Warning 15%")
        fig.add_hline(y=40, line_dash="dash", line_color="red", annotation_text="Critical 40%")
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for events...")

# ── Row 2: Latency + Token Usage ──────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("Avg Latency by Model (ms)")
    df = q(f"""
        SELECT
            date_trunc('minute', ts) AS time,
            model,
            ROUND(AVG(latency_ms)::numeric, 0) AS avg_latency_ms
        FROM llm_events
        WHERE ts >= NOW() - INTERVAL '{interval}' AND status = 'success'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.line(
            df,
            x="time",
            y="avg_latency_ms",
            color="model",
            labels={"time": "", "avg_latency_ms": "Avg Latency (ms)", "model": "Model"},
        )
        fig.add_hline(y=1000, line_dash="dash", line_color="orange", annotation_text="Warning 1s")
        fig.add_hline(y=3000, line_dash="dash", line_color="red", annotation_text="Critical 3s")
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for events...")

with col4:
    st.subheader("Token Usage by Model")
    df = q(f"""
        SELECT
            date_trunc('minute', ts) AS time,
            model,
            SUM(total_tokens) AS total_tokens
        FROM llm_events
        WHERE ts >= NOW() - INTERVAL '{interval}'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.line(
            df,
            x="time",
            y="total_tokens",
            color="model",
            labels={"time": "", "total_tokens": "Tokens / min", "model": "Model"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for events...")

# ── Row 3: Cost Over Time + Total Cost Bar ────────────────────────────────────
col5, col6 = st.columns(2)

with col5:
    st.subheader("Cost Over Time by Model (USD)")
    df = q(f"""
        SELECT
            date_trunc('minute', ts) AS time,
            model,
            ROUND(SUM(cost_usd)::numeric, 6) AS cost_usd
        FROM llm_events
        WHERE ts >= NOW() - INTERVAL '{interval}'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not df.empty:
        fig = px.area(
            df,
            x="time",
            y="cost_usd",
            color="model",
            labels={"time": "", "cost_usd": "Cost (USD)", "model": "Model"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for events...")

with col6:
    st.subheader("Total Cost by Model (USD)")
    df = q(f"""
        SELECT model, ROUND(SUM(total_cost_usd)::numeric, 4) AS total_cost
        FROM cost_rollups_hourly
        WHERE window_start >= NOW() - INTERVAL '{interval}'
        GROUP BY model
        ORDER BY total_cost DESC
    """)
    if not df.empty:
        fig = px.bar(
            df,
            x="model",
            y="total_cost",
            color="model",
            labels={"model": "Model", "total_cost": "Total Cost (USD)"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No rollup data yet — Flink window has not closed.")

st.divider()

# ── Anomaly Events ────────────────────────────────────────────────────────────
st.subheader("Anomaly Events")

df_anomalies = q(f"""
    SELECT
        detected_at,
        anomaly_type,
        severity,
        model,
        user_id,
        ROUND(metric_value::numeric,    4) AS metric_value,
        ROUND(threshold_value::numeric, 4) AS threshold_value,
        window_start,
        window_end
    FROM anomaly_events
    WHERE detected_at >= NOW() - INTERVAL '{interval}'
    ORDER BY detected_at DESC
    LIMIT 100
""")

if not df_anomalies.empty:

    def _sev_color(val: str) -> str:
        if val == "critical":
            return "background-color: #ff4b4b; color: white"
        if val == "warning":
            return "background-color: #ffa726; color: white"
        return ""

    def _type_color(val: str) -> str:
        return {
            "latency_spike": "color: #ff7043",
            "error_surge": "color: #ef5350",
            "cost_spike": "color: #ab47bc",
        }.get(val, "")

    styled = df_anomalies.style.map(_sev_color, subset=["severity"]).map(
        _type_color, subset=["anomaly_type"]
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.success("No anomalies detected in the selected time range.")

st.divider()

# ── Live Event Feed ────────────────────────────────────────────────────────────
st.subheader("Live Event Feed")

df_live = q(f"""
    SELECT
        ts                              AS time,
        model,
        user_id,
        prompt_tokens,
        completion_tokens,
        ROUND(cost_usd::numeric, 6)     AS cost_usd,
        latency_ms,
        status,
        COALESCE(error_code, '')        AS error_code
    FROM llm_events
    WHERE ts >= NOW() - INTERVAL '{interval}'
    ORDER BY ts DESC
    LIMIT 100
""")

if not df_live.empty:

    def _status_color(val: str) -> str:
        return {
            "success": "color: #66bb6a",
            "error": "color: #ef5350",
            "timeout": "color: #ffa726",
        }.get(val, "")

    styled_live = df_live.style.map(_status_color, subset=["status"])
    st.dataframe(styled_live, use_container_width=True, hide_index=True)
else:
    st.info("Waiting for events...")
