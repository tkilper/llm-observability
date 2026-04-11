"""
Simulated LLM API call event producer.

Generates realistic LLM call events across multiple models, users, and
sessions. Injects anomaly bursts (latency spikes, error surges, token spikes)
at a configurable rate to exercise downstream Flink detection logic.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))
TOPIC = "llm.events.raw"
ANOMALY_BURST_PROBABILITY = 0.04   # 4% chance per event to trigger a burst
ANOMALY_BURST_SIZE = (5, 20)       # burst lasts 5-20 events

# ── Model catalogue with pricing (per 1k tokens) ──────────────────────────────
MODELS: dict[str, dict] = {
    "gpt-4o": {
        "provider": "openai",
        "input_cost_per_1k": 0.0025,
        "output_cost_per_1k": 0.01000,
        "base_latency_ms": 800,
        "weight": 0.28,
    },
    "gpt-4o-mini": {
        "provider": "openai",
        "input_cost_per_1k": 0.000150,
        "output_cost_per_1k": 0.000600,
        "base_latency_ms": 350,
        "weight": 0.25,
    },
    "gpt-3.5-turbo": {
        "provider": "openai",
        "input_cost_per_1k": 0.000500,
        "output_cost_per_1k": 0.001500,
        "base_latency_ms": 280,
        "weight": 0.15,
    },
    "claude-3-5-sonnet-20241022": {
        "provider": "anthropic",
        "input_cost_per_1k": 0.003,
        "output_cost_per_1k": 0.015,
        "base_latency_ms": 950,
        "weight": 0.17,
    },
    "claude-3-haiku-20240307": {
        "provider": "anthropic",
        "input_cost_per_1k": 0.000250,
        "output_cost_per_1k": 0.001250,
        "base_latency_ms": 320,
        "weight": 0.10,
    },
    "gemini-1.5-pro": {
        "provider": "google",
        "input_cost_per_1k": 0.00125,
        "output_cost_per_1k": 0.005,
        "base_latency_ms": 600,
        "weight": 0.05,
    },
}

MODEL_NAMES = list(MODELS.keys())
MODEL_WEIGHTS = [MODELS[m]["weight"] for m in MODEL_NAMES]

# ── Simulated users & sessions ────────────────────────────────────────────────
USERS = [f"user_{i:03d}" for i in range(1, 21)]
USER_WEIGHTS = [random.uniform(0.5, 3.0) for _ in USERS]
SESSIONS: dict[str, list[str]] = {
    user: [str(uuid.uuid4()) for _ in range(random.randint(1, 4))]
    for user in USERS
}

ERROR_CODES = [
    "rate_limit_exceeded",
    "invalid_request_error",
    "context_length_exceeded",
    "server_error",
]


def _calc_cost(model_name: str, prompt_tokens: int, completion_tokens: int) -> float:
    m = MODELS[model_name]
    return (prompt_tokens / 1000) * m["input_cost_per_1k"] + (
        completion_tokens / 1000
    ) * m["output_cost_per_1k"]


def _sample_tokens() -> tuple[int, int]:
    """Sample prompt and completion token counts from log-normal distributions."""
    prompt = int(np.clip(np.random.lognormal(mean=5.5, sigma=1.2), 10, 32_000))
    completion = int(np.clip(np.random.lognormal(mean=4.5, sigma=1.0), 5, 4_096))
    return prompt, completion


def generate_event(anomaly_type: str | None = None) -> dict:
    model_name = random.choices(MODEL_NAMES, weights=MODEL_WEIGHTS)[0]
    model = MODELS[model_name]
    user_id = random.choices(USERS, weights=USER_WEIGHTS)[0]
    session_id = random.choice(SESSIONS[user_id])

    prompt_tokens, completion_tokens = _sample_tokens()
    base_latency = model["base_latency_ms"]
    latency_ms = int(
        base_latency
        + completion_tokens * 2.5
        + np.random.normal(0, base_latency * 0.2)
    )
    status = "success"
    error_code = None

    if anomaly_type == "latency_spike":
        latency_ms = int(latency_ms * random.uniform(6, 18))
    elif anomaly_type == "error_burst":
        status = random.choice(["error", "timeout"])
        latency_ms = int(latency_ms * random.uniform(1.0, 3.0))
        error_code = (
            random.choice(ERROR_CODES) if status == "error" else "request_timeout"
        )
    elif anomaly_type == "token_spike":
        prompt_tokens = min(32_000, prompt_tokens * random.randint(5, 20))
        completion_tokens = min(4_096, completion_tokens * random.randint(3, 8))
    else:
        # Normal distribution of errors
        rand = random.random()
        if rand >= 0.97:
            status = "timeout"
            error_code = "request_timeout"
            latency_ms = int(latency_ms * random.uniform(2, 4))
        elif rand >= 0.94:
            status = "error"
            error_code = random.choice(ERROR_CODES)

    latency_ms = max(50, latency_ms)
    cost_usd = _calc_cost(model_name, prompt_tokens, completion_tokens) if status == "success" else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00",
        "model": model_name,
        "provider": model["provider"],
        "user_id": user_id,
        "session_id": session_id,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "cost_usd": round(cost_usd, 6),
        "latency_ms": latency_ms,
        "status": status,
        "error_code": error_code,
        "request_id": str(uuid.uuid4()),
    }


def on_delivery(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "linger.ms": 50,
            "batch.size": 16384,
            "compression.type": "lz4",
        }
    )
    log.info("Producer started — %.1f events/sec → topic '%s'", EVENTS_PER_SECOND, TOPIC)

    interval = 1.0 / EVENTS_PER_SECOND
    burst_remaining = 0
    current_anomaly: str | None = None

    try:
        while True:
            loop_start = time.monotonic()

            if burst_remaining > 0:
                event = generate_event(anomaly_type=current_anomaly)
                burst_remaining -= 1
                if burst_remaining == 0:
                    current_anomaly = None
                    log.info("Anomaly burst ended.")
            elif random.random() < ANOMALY_BURST_PROBABILITY:
                current_anomaly = random.choice(
                    ["latency_spike", "error_burst", "token_spike"]
                )
                burst_remaining = random.randint(*ANOMALY_BURST_SIZE) - 1
                event = generate_event(anomaly_type=current_anomaly)
                log.info("Anomaly burst started: %s (%d events)", current_anomaly, burst_remaining + 1)
            else:
                event = generate_event()

            producer.produce(
                TOPIC,
                key=event["user_id"].encode(),
                value=json.dumps(event).encode(),
                callback=on_delivery,
            )
            producer.poll(0)

            elapsed = time.monotonic() - loop_start
            time.sleep(max(0.0, interval - elapsed))

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        producer.flush(timeout=10)
        log.info("Producer flushed and closed.")


if __name__ == "__main__":
    main()
