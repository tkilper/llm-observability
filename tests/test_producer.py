"""
Unit tests for producer.py business logic.

Covers cost calculation, token sampling, and event generation — including
all three anomaly injection types (latency_spike, error_burst, token_spike).
"""

import pytest

import producer as p  # conftest.py sets sys.path and stubs confluent_kafka

# ── Cost calculation ──────────────────────────────────────────────────────────


class TestCalcCost:
    def test_gpt4o_known_values(self):
        # gpt-4o: $0.0025/1k input + $0.01/1k output
        cost = p._calc_cost("gpt-4o", 1_000, 1_000)
        assert cost == pytest.approx(0.0025 + 0.01)

    def test_zero_tokens_is_zero_cost(self):
        for model in p.MODELS:
            assert p._calc_cost(model, 0, 0) == 0.0

    def test_cost_scales_linearly_with_tokens(self):
        cost_1x = p._calc_cost("claude-3-5-sonnet-20241022", 100, 100)
        cost_2x = p._calc_cost("claude-3-5-sonnet-20241022", 200, 200)
        assert cost_2x == pytest.approx(cost_1x * 2)

    def test_all_models_produce_positive_cost(self):
        for model in p.MODELS:
            assert p._calc_cost(model, 500, 200) > 0

    def test_input_and_output_rates_are_independent(self):
        # Doubling only prompt tokens should not change completion cost
        base = p._calc_cost("gpt-4o-mini", 100, 100)
        more_input = p._calc_cost("gpt-4o-mini", 200, 100)
        more_output = p._calc_cost("gpt-4o-mini", 100, 200)
        assert more_input != more_output  # different pricing tiers


# ── Token sampling ────────────────────────────────────────────────────────────


class TestSampleTokens:
    def test_returns_two_integers(self):
        prompt, completion = p._sample_tokens()
        assert isinstance(prompt, int)
        assert isinstance(completion, int)

    def test_prompt_tokens_within_bounds(self):
        for _ in range(200):
            prompt, _ = p._sample_tokens()
            assert 10 <= prompt <= 32_000, f"prompt_tokens={prompt} out of bounds"

    def test_completion_tokens_within_bounds(self):
        for _ in range(200):
            _, completion = p._sample_tokens()
            assert 5 <= completion <= 4_096, f"completion_tokens={completion} out of bounds"


# ── Normal event generation ───────────────────────────────────────────────────


class TestGenerateEventNormal:
    REQUIRED_FIELDS = {
        "event_id",
        "timestamp",
        "model",
        "provider",
        "user_id",
        "session_id",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cost_usd",
        "latency_ms",
        "status",
        "error_code",
        "request_id",
    }

    def test_event_contains_all_required_fields(self):
        event = p.generate_event()
        assert self.REQUIRED_FIELDS.issubset(event.keys())

    def test_total_tokens_equals_sum_of_parts(self):
        for _ in range(20):
            event = p.generate_event()
            assert event["total_tokens"] == event["prompt_tokens"] + event["completion_tokens"]

    def test_model_is_in_catalogue(self):
        for _ in range(20):
            assert p.generate_event()["model"] in p.MODELS

    def test_status_is_valid_value(self):
        valid = {"success", "error", "timeout"}
        for _ in range(50):
            assert p.generate_event()["status"] in valid

    def test_latency_has_minimum_floor(self):
        for _ in range(50):
            assert p.generate_event()["latency_ms"] >= 50

    def test_success_events_have_nonzero_cost(self):
        success_events = [e for _ in range(100) if (e := p.generate_event())["status"] == "success"]
        assert success_events, "Expected at least one success event in 100 trials"
        for event in success_events:
            assert event["cost_usd"] > 0

    def test_non_success_events_have_zero_cost(self):
        non_success = [e for _ in range(200) if (e := p.generate_event())["status"] != "success"]
        for event in non_success:
            assert event["cost_usd"] == 0.0

    def test_normal_error_rate_is_low(self):
        # Natural error rate is ~6% (3% error + 3% timeout from rand thresholds 0.94/0.97)
        statuses = [p.generate_event()["status"] for _ in range(300)]
        error_rate = 1 - statuses.count("success") / len(statuses)
        assert error_rate < 0.20, f"Error rate {error_rate:.1%} unexpectedly high for normal events"


# ── Anomaly: latency spike ────────────────────────────────────────────────────


class TestLatencySpikeAnomaly:
    def test_latency_spike_multiplies_latency(self):
        # Spike multiplies base latency by 6–18x, so spike avg >> normal avg
        spike_latencies = [
            p.generate_event(anomaly_type="latency_spike")["latency_ms"] for _ in range(30)
        ]
        normal_latencies = [p.generate_event()["latency_ms"] for _ in range(30)]
        assert sum(spike_latencies) / 30 > sum(normal_latencies) / 30 * 3

    def test_latency_spike_preserves_success_status(self):
        # Latency spikes do not change the status — they just slow down
        events = [p.generate_event(anomaly_type="latency_spike") for _ in range(30)]
        # Most should still be success (latency spike doesn't set error status)
        success_count = sum(1 for e in events if e["status"] == "success")
        assert success_count > 20


# ── Anomaly: error burst ──────────────────────────────────────────────────────


class TestErrorBurstAnomaly:
    def test_error_burst_produces_non_success_statuses(self):
        statuses = {p.generate_event(anomaly_type="error_burst")["status"] for _ in range(30)}
        assert statuses & {"error", "timeout"}, "Expected error or timeout statuses in error_burst"

    def test_error_events_have_error_code(self):
        for _ in range(30):
            event = p.generate_event(anomaly_type="error_burst")
            if event["status"] == "error":
                assert event["error_code"] in p.ERROR_CODES

    def test_timeout_events_have_timeout_error_code(self):
        for _ in range(30):
            event = p.generate_event(anomaly_type="error_burst")
            if event["status"] == "timeout":
                assert event["error_code"] == "request_timeout"

    def test_error_burst_events_have_zero_cost(self):
        for _ in range(30):
            event = p.generate_event(anomaly_type="error_burst")
            if event["status"] != "success":
                assert event["cost_usd"] == 0.0


# ── Anomaly: token spike ──────────────────────────────────────────────────────


class TestTokenSpikeAnomaly:
    def test_token_spike_respects_prompt_max(self):
        for _ in range(30):
            event = p.generate_event(anomaly_type="token_spike")
            assert event["prompt_tokens"] <= 32_000

    def test_token_spike_respects_completion_max(self):
        for _ in range(30):
            event = p.generate_event(anomaly_type="token_spike")
            assert event["completion_tokens"] <= 4_096

    def test_token_spike_inflates_token_counts(self):
        spike_totals = [
            p.generate_event(anomaly_type="token_spike")["total_tokens"] for _ in range(30)
        ]
        normal_totals = [p.generate_event()["total_tokens"] for _ in range(30)]
        assert sum(spike_totals) / 30 > sum(normal_totals) / 30
