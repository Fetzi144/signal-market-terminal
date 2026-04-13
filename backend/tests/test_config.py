"""Tests for config validation."""
import pytest
from pydantic import ValidationError

from app.config import Settings


def _make(**overrides) -> Settings:
    """Build a Settings instance with overrides, bypassing .env file."""
    defaults = {
        "database_url": "postgresql+asyncpg://test:test@localhost:5432/test",
    }
    defaults.update(overrides)
    return Settings(**defaults, _env_file=None)


class TestIntervalValidation:
    def test_valid_intervals(self):
        s = _make(snapshot_interval_seconds=60, market_discovery_interval_seconds=120, evaluation_interval_seconds=300)
        assert s.snapshot_interval_seconds == 60

    def test_interval_below_minimum_rejected(self):
        with pytest.raises(ValidationError, match="Interval must be >= 30"):
            _make(snapshot_interval_seconds=10)

    def test_interval_at_boundary(self):
        s = _make(snapshot_interval_seconds=30)
        assert s.snapshot_interval_seconds == 30


class TestRetentionValidation:
    def test_valid_retention(self):
        s = _make(retention_price_snapshots_days=7)
        assert s.retention_price_snapshots_days == 7

    def test_zero_retention_rejected(self):
        with pytest.raises(ValidationError, match="Retention must be >= 1"):
            _make(retention_price_snapshots_days=0)


class TestThresholdValidation:
    def test_valid_thresholds(self):
        s = _make(price_move_threshold_pct=10.0, alert_rank_threshold=0.5, shadow_execution_min_fill_pct=0.2)
        assert s.price_move_threshold_pct == 10.0

    def test_zero_threshold_rejected(self):
        with pytest.raises(ValidationError, match="Threshold must be > 0"):
            _make(price_move_threshold_pct=0.0)

    def test_negative_threshold_rejected(self):
        with pytest.raises(ValidationError, match="Threshold must be > 0"):
            _make(volume_spike_multiplier=-1.0)


class TestAlertRankThresholdBounds:
    def test_valid_threshold(self):
        s = _make(alert_rank_threshold=0.7)
        assert s.alert_rank_threshold == 0.7

    def test_zero_threshold_allowed(self):
        s = _make(alert_rank_threshold=0.0)
        assert s.alert_rank_threshold == 0.0

    def test_one_threshold_allowed(self):
        s = _make(alert_rank_threshold=1.0)
        assert s.alert_rank_threshold == 1.0

    def test_above_one_rejected(self):
        with pytest.raises(ValidationError, match="alert_rank_threshold must be between 0.0 and 1.0"):
            _make(alert_rank_threshold=1.1)

    def test_negative_rejected(self):
        with pytest.raises(ValidationError, match="alert_rank_threshold must be between 0.0 and 1.0"):
            _make(alert_rank_threshold=-0.1)


class TestShadowExecutionBounds:
    def test_shadow_execution_min_fill_pct_accepts_one(self):
        s = _make(shadow_execution_min_fill_pct=1.0)
        assert s.shadow_execution_min_fill_pct == 1.0

    def test_shadow_execution_min_fill_pct_rejects_above_one(self):
        with pytest.raises(ValidationError, match="shadow_execution_min_fill_pct must be > 0 and <= 1"):
            _make(shadow_execution_min_fill_pct=1.1)


class TestLimitValidation:
    def test_valid_limits(self):
        s = _make(alert_batch_limit=10, market_pagination_cap=3000)
        assert s.alert_batch_limit == 10

    def test_zero_limit_rejected(self):
        with pytest.raises(ValidationError, match="Limit must be >= 1"):
            _make(alert_batch_limit=0)


class TestSseMaxConnections:
    def test_valid_sse_max_connections(self):
        s = _make(sse_max_connections=100)
        assert s.sse_max_connections == 100

    def test_default_sse_max_connections(self):
        s = _make()
        assert s.sse_max_connections == 50

    def test_zero_sse_max_connections_rejected(self):
        with pytest.raises(ValidationError, match="sse_max_connections must be >= 1"):
            _make(sse_max_connections=0)

    def test_negative_sse_max_connections_rejected(self):
        with pytest.raises(ValidationError, match="sse_max_connections must be >= 1"):
            _make(sse_max_connections=-1)


class TestDefaults:
    def test_default_values(self):
        s = _make()
        assert s.alert_batch_limit == 20
        assert s.market_pagination_cap == 100000
        assert s.orderbook_sample_size == 50
        assert s.cleanup_interval_hours == 6
        assert s.kalshi_enabled is True
        assert s.sse_max_connections == 50
        assert s.alert_webhook_secret == ""
        assert s.shadow_execution_max_staleness_seconds == 180
        assert s.shadow_execution_max_forward_seconds == 30
        assert s.shadow_execution_min_fill_pct == 0.20
