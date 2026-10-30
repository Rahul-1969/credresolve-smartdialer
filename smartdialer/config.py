"""Configuration settings for SmartDialer runtime and pacing."""

import time
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class Config:
    """Runtime configuration for SmartDialer."""

    db_path: str = "smartdialer.db"
    lease_seconds: int = 30
    agent_reservation_lease_seconds: float = 30.0
    borrower_claim_lease_seconds: float = 30.0
    max_attempts: int = 3
    retry_backoff_seconds: float = 60.0
    reaper_interval: float = 5.0
    max_overdial_ratio: float = 1.5
    target_abandonment_rate: float = 0.03
    progressive_fallback_threshold: float = 0.05
    default_provider: str = "provider_a"
    log_level: str = "INFO"
    abandon_retry_delay_s: float = 300.0
    nonpenalized_retry_delay_s: float = 5.0
    action_claim_lease_s: float = 15.0
    action_max_attempts: int = 10
    clock: Callable[[], float] = field(default=time.time)
