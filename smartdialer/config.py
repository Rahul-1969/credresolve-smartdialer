"""Configuration settings for SmartDialer runtime and pacing."""

from dataclasses import dataclass


@dataclass
class Config:
    """Runtime configuration for SmartDialer."""

    db_path: str = "smartdialer.db"
    lease_seconds: int = 30
    reaper_interval: float = 5.0
    max_overdial_ratio: float = 1.5
    target_abandonment_rate: float = 0.03
    progressive_fallback_threshold: float = 0.05
    default_provider: str = "provider_a"
    log_level: str = "INFO"

