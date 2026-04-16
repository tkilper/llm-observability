"""
Tests for anomaly detection threshold logic.

These functions mirror the CASE WHEN expressions in
flink/jobs/anomaly_detection.py. If thresholds change in the SQL, update
here too — these tests serve as executable documentation of the business rules.
"""


def _latency_severity(avg_latency_ms: float) -> str | None:
    if avg_latency_ms >= 6000.0:
        return "critical"
    if avg_latency_ms >= 3000.0:
        return "warning"
    return None


def _error_severity(error_rate: float) -> str | None:
    if error_rate >= 0.40:
        return "critical"
    if error_rate >= 0.15:
        return "warning"
    return None


def _cost_severity(total_cost_usd: float) -> str | None:
    if total_cost_usd >= 5.0:
        return "critical"
    if total_cost_usd >= 1.0:
        return "warning"
    return None


# ── Latency thresholds ────────────────────────────────────────────────────────

class TestLatencyThresholds:
    def test_below_warning_is_no_anomaly(self):
        assert _latency_severity(2999.9) is None

    def test_at_warning_threshold(self):
        assert _latency_severity(3000.0) == "warning"

    def test_between_thresholds_is_warning(self):
        assert _latency_severity(4500.0) == "warning"

    def test_just_below_critical_is_warning(self):
        assert _latency_severity(5999.9) == "warning"

    def test_at_critical_threshold(self):
        assert _latency_severity(6000.0) == "critical"

    def test_above_critical_is_critical(self):
        assert _latency_severity(12000.0) == "critical"

    def test_zero_latency_is_no_anomaly(self):
        assert _latency_severity(0.0) is None


# ── Error rate thresholds ─────────────────────────────────────────────────────

class TestErrorRateThresholds:
    def test_below_warning_is_no_anomaly(self):
        assert _error_severity(0.14) is None

    def test_at_warning_threshold(self):
        assert _error_severity(0.15) == "warning"

    def test_between_thresholds_is_warning(self):
        assert _error_severity(0.25) == "warning"

    def test_just_below_critical_is_warning(self):
        assert _error_severity(0.399) == "warning"

    def test_at_critical_threshold(self):
        assert _error_severity(0.40) == "critical"

    def test_above_critical_is_critical(self):
        assert _error_severity(1.0) == "critical"

    def test_zero_error_rate_is_no_anomaly(self):
        assert _error_severity(0.0) is None


# ── Cost thresholds ───────────────────────────────────────────────────────────

class TestCostThresholds:
    def test_below_warning_is_no_anomaly(self):
        assert _cost_severity(0.99) is None

    def test_at_warning_threshold(self):
        assert _cost_severity(1.0) == "warning"

    def test_between_thresholds_is_warning(self):
        assert _cost_severity(3.0) == "warning"

    def test_just_below_critical_is_warning(self):
        assert _cost_severity(4.99) == "warning"

    def test_at_critical_threshold(self):
        assert _cost_severity(5.0) == "critical"

    def test_above_critical_is_critical(self):
        assert _cost_severity(100.0) == "critical"

    def test_zero_cost_is_no_anomaly(self):
        assert _cost_severity(0.0) is None
