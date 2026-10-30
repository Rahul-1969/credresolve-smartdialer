"""Provider B factory: budget telecom provider with latency, errors, and delivery anomalies.

Single Responsibility:
    Instantiate Provider B with higher latency, errors, timeouts, out-of-order delivery,
    and duplicate event emission to thoroughly test dialer resilience.
"""

import random

from smartdialer.providers.behavior import BorrowerBehavior
from smartdialer.providers.mock import MockProvider, ProviderProfile


def make_provider_b(rng: random.Random, behavior: BorrowerBehavior) -> MockProvider:
    """Create configured Provider B instance."""
    profile = ProviderProfile(
        setup_latency_s=1.5,
        latency_jitter_s=1.0,
        error_rate=0.02,
        timeout_rate=0.05,
        duplicate_rate=0.15,
        reorder_rate=0.15,
        reorder_jitter_s=8.0,
    )
    return MockProvider(name="provider_b", profile=profile, behavior=behavior, rng=rng)
