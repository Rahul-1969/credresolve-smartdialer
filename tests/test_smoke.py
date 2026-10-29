"""Smoke tests for SmartDialer package and configuration defaults."""

import smartdialer
from smartdialer.config import Config


def test_package_import():
    """Assert smartdialer imports and exposes version."""
    assert smartdialer.__version__ is not None
    assert isinstance(smartdialer.__version__, str)


def test_config_defaults():
    """Assert Config instantiates with expected default values."""
    config = Config()
    assert config.db_path == "smartdialer.db"
    assert config.lease_seconds == 30
    assert config.agent_reservation_lease_seconds == 30.0
    assert config.borrower_claim_lease_seconds == 30.0
    assert config.reaper_interval == 5.0
    assert config.max_overdial_ratio == 1.5
    assert config.target_abandonment_rate == 0.03
    assert config.progressive_fallback_threshold == 0.05
    assert config.default_provider == "provider_a"
    assert config.log_level == "INFO"
    assert callable(config.clock)
