"""Provider A factory: low-latency, high-reliability telecom provider.

Single Responsibility:
    Instantiate Provider A with realistic tier-1 carrier parameters (low latency,
    minimal error rate, zero timeout, no delivery jitter or duplicates).
"""

import random

from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile


def make_provider_a(rng: random.Random, behavior: BorrowerBehavior) -> MockProvider:
    """Create configured Provider A instance."""
    profile = ProviderProfile(
        setup_latency_s=0.3,
        latency_jitter_s=0.1,
        error_rate=0.005,
        timeout_rate=0.0,
        duplicate_rate=0.0,
        reorder_rate=0.0,
    )
    return MockProvider(name="provider_a", profile=profile, behavior=behavior, rng=rng)
