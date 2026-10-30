"""Borrower behavioral response models for simulated telecom calling.

Single Responsibility:
    Sample realistic borrower interaction outcomes (answer probability,
    ringing duration before answer, and talk duration) based on static or
    time-dependent behavioral profiles.
"""

from dataclasses import dataclass
import random
from typing import Callable


@dataclass(frozen=True)
class Outcome:
    """Sampled outcome of a call attempt to a borrower."""

    answers: bool
    ring_s: float
    talk_s: float


@dataclass
class BorrowerBehavior:
    """Configurable borrower behavior profile."""

    answer_rate: float | Callable[[float], float]
    mean_talk_s: float | Callable[[float], float]

    def sample(self, rng: random.Random, now: float) -> Outcome:
        """Sample call outcome at current virtual time."""
        rate = self.answer_rate(now) if callable(self.answer_rate) else self.answer_rate
        mean_talk = self.mean_talk_s(now) if callable(self.mean_talk_s) else self.mean_talk_s

        answers = rng.random() < rate
        ring_s = rng.uniform(2.0, 8.0) if answers else 0.0

        if answers:
            if mean_talk > 0:
                raw_talk = rng.expovariate(1.0 / mean_talk)
                talk_s = max(5.0, min(4.0 * mean_talk, raw_talk))
            else:
                talk_s = 5.0
        else:
            talk_s = 0.0

        return Outcome(answers=answers, ring_s=ring_s, talk_s=talk_s)

