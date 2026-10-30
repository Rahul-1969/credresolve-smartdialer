"""Configurable mock telecom provider simulating latencies, jitter, errors, and outages.

Single Responsibility:
    Simulate realistic telecom provider behavior with pure virtual time, deterministic
    randomness, idempotent call placement, out-of-order/duplicate event delivery,
    cancellations, and simulated provider outages.
"""

from dataclasses import dataclass, field
import random
from typing import Any

from smartdialer.providers.base import (
    PlaceResult,
    ProviderError,
    ProviderEvent,
    ProviderTimeout,
    TelecomProvider,
)
from smartdialer.providers.behavior import BorrowerBehavior, Outcome


@dataclass
class ProviderProfile:
    """Operational profile defining latency, failure rates, and delivery anomalies."""

    setup_latency_s: float
    latency_jitter_s: float
    error_rate: float
    timeout_rate: float
    timeout_places_call_prob: float = 0.5
    duplicate_rate: float = 0.0
    max_extra_copies: int = 2
    reorder_rate: float = 0.0
    reorder_jitter_s: float = 0.0
    ring_timeout_s: float = 25.0


@dataclass
class Outage:
    """Injected provider outage window and characteristics."""

    start: float
    end: float
    mode: str  # "timeout" or "error"
    events: str  # "delay" or "drop"


@dataclass
class CallGroundTruth:
    """Internal provider ground truth for a placed call."""

    call_id: str
    phone: str
    placed_at: float
    setup_latency: float
    ringing_at: float
    outcome: Outcome
    answered_at: float | None = None
    completed_at: float | None = None
    failed_at: float | None = None
    cancelled_at: float | None = None


@dataclass
class _PendingEventItem:
    """Internal wrapper for delivery queue ordering."""

    deliver_at: float
    seq: int
    event: ProviderEvent


class MockProvider(TelecomProvider):
    """Deterministic simulated telecom provider."""

    def __init__(
        self,
        name: str,
        profile: ProviderProfile,
        behavior: BorrowerBehavior,
        rng: random.Random,
    ) -> None:
        self.name = name
        self.profile = profile
        self.behavior = behavior
        self.rng = rng

        self._seq_counter: int = 0
        self._placed_calls: dict[str, PlaceResult] = {}
        self._call_records: dict[str, CallGroundTruth] = {}
        self._pending_events: list[_PendingEventItem] = []
        self._outages: list[Outage] = []

    def set_profile(self, **changes: Any) -> None:
        """Dynamically update profile configuration at runtime."""
        for k, v in changes.items():
            if hasattr(self.profile, k):
                setattr(self.profile, k, v)
            else:
                raise AttributeError(f"ProviderProfile has no attribute '{k}'")

    def inject_outage(self, start: float, end: float, mode: str, events: str) -> None:
        """Inject a scheduled outage window."""
        if mode not in {"timeout", "error"}:
            raise ValueError(f"Invalid outage mode: {mode}. Must be 'timeout' or 'error'.")
        if events not in {"delay", "drop"}:
            raise ValueError(f"Invalid outage events policy: {events}. Must be 'delay' or 'drop'.")
        self._outages.append(Outage(start=start, end=end, mode=mode, events=events))

    def _next_seq(self) -> int:
        self._seq_counter += 1
        return self._seq_counter

    def _get_active_outage(self, now: float) -> Outage | None:
        for o in self._outages:
            if o.start <= now < o.end:
                return o
        return None

    def _do_place(self, call_id: str, phone: str, now: float) -> PlaceResult:
        """Internal helper to execute physical call placement and schedule events."""
        jitter = self.rng.uniform(-self.profile.latency_jitter_s, self.profile.latency_jitter_s)
        latency = max(0.01, self.profile.setup_latency_s + jitter)

        provider_call_id = f"{self.name}_{call_id}"
        place_res = PlaceResult(provider_call_id=provider_call_id, latency_s=latency)
        self._placed_calls[call_id] = place_res

        outcome = self.behavior.sample(self.rng, now)
        ringing_at = now + latency

        record = CallGroundTruth(
            call_id=call_id,
            phone=phone,
            placed_at=now,
            setup_latency=latency,
            ringing_at=ringing_at,
            outcome=outcome,
        )

        scheduled_events: list[tuple[str, str | None, float]] = []
        # (event_type, reason, occurred_at)
        scheduled_events.append(("RINGING", None, ringing_at))

        if outcome.answers:
            answered_at = ringing_at + outcome.ring_s
            completed_at = answered_at + outcome.talk_s
            record.answered_at = answered_at
            record.completed_at = completed_at
            scheduled_events.append(("ANSWERED", None, answered_at))
            scheduled_events.append(("COMPLETED", None, completed_at))
        else:
            failed_at = now + latency + self.profile.ring_timeout_s
            record.failed_at = failed_at
            scheduled_events.append(("FAILED", "NO_ANSWER", failed_at))

        self._call_records[call_id] = record

        # Queue events with potential reordering and duplicates
        for event_type, reason, occurred_at in scheduled_events:
            deliver_at = occurred_at
            if self.profile.reorder_rate > 0.0 and self.rng.random() < self.profile.reorder_rate:
                deliver_at += self.rng.uniform(0.0, self.profile.reorder_jitter_s)

            event_id = f"{self.name}:{call_id}:{event_type}"
            base_evt = ProviderEvent(
                provider=self.name,
                call_id=call_id,
                provider_call_id=provider_call_id,
                provider_event_id=event_id,
                event_type=event_type,
                reason=reason,
                occurred_at=occurred_at,
                deliver_at=deliver_at,
            )
            self._pending_events.append(
                _PendingEventItem(deliver_at=deliver_at, seq=self._next_seq(), event=base_evt)
            )

            if self.profile.duplicate_rate > 0.0 and self.rng.random() < self.profile.duplicate_rate:
                copies = self.rng.randint(1, self.profile.max_extra_copies)
                for _ in range(copies):
                    dup_deliver = deliver_at + self.rng.uniform(0.1, 3.0)
                    dup_evt = ProviderEvent(
                        provider=self.name,
                        call_id=call_id,
                        provider_call_id=provider_call_id,
                        provider_event_id=event_id,
                        event_type=event_type,
                        reason=reason,
                        occurred_at=occurred_at,
                        deliver_at=dup_deliver,
                    )
                    self._pending_events.append(
                        _PendingEventItem(
                            deliver_at=dup_deliver,
                            seq=self._next_seq(),
                            event=dup_evt,
                        )
                    )

        return place_res

    def place_call(self, call_id: str, phone: str, now: float) -> PlaceResult:
        """Place an outbound call according to strictly ordered check rules."""
        # (1) Active outage check
        outage = self._get_active_outage(now)
        if outage is not None:
            if outage.mode == "error":
                raise ProviderError(f"Provider {self.name} active outage: error")
            if outage.mode == "timeout":
                if call_id not in self._placed_calls:
                    if self.rng.random() < self.profile.timeout_places_call_prob:
                        self._do_place(call_id, phone, now)
                raise ProviderTimeout(f"Provider {self.name} active outage: timeout")

        # (2) Idempotency check: known call_id returns same PlaceResult, schedules nothing new
        if call_id in self._placed_calls:
            return self._placed_calls[call_id]

        # (3) Error rate check -> ProviderError (not placed)
        if self.profile.error_rate > 0.0 and self.rng.random() < self.profile.error_rate:
            raise ProviderError(f"Provider {self.name} call placement failed")

        # (4) Timeout rate check -> with prob timeout_places_call_prob actually place call, then raise ProviderTimeout
        if self.profile.timeout_rate > 0.0 and self.rng.random() < self.profile.timeout_rate:
            if self.rng.random() < self.profile.timeout_places_call_prob:
                self._do_place(call_id, phone, now)
            raise ProviderTimeout(f"Provider {self.name} call placement timed out")

        # (5) Normal placement
        return self._do_place(call_id, phone, now)

    def cancel_call(self, call_id: str, now: float) -> bool:
        """Cancel an in-flight call if not already answered/terminal."""
        if call_id not in self._call_records:
            return False

        status = self.get_call_status(call_id, now)
        if status in {"ANSWERED", "COMPLETED", "FAILED", "CANCELLED"}:
            return False

        record = self._call_records[call_id]

        # Drop not-yet-occurred events for this call
        self._pending_events = [
            item
            for item in self._pending_events
            if not (item.event.call_id == call_id and item.event.occurred_at > now)
        ]

        # Emit CANCELLED at now + latency
        jitter = self.rng.uniform(-self.profile.latency_jitter_s, self.profile.latency_jitter_s)
        latency = max(0.01, self.profile.setup_latency_s + jitter)
        cancelled_at = now + latency
        record.cancelled_at = cancelled_at

        provider_call_id = self._placed_calls[call_id].provider_call_id
        cancel_evt = ProviderEvent(
            provider=self.name,
            call_id=call_id,
            provider_call_id=provider_call_id,
            provider_event_id=f"{self.name}:{call_id}:CANCELLED",
            event_type="CANCELLED",
            reason="CANCELLED",
            occurred_at=cancelled_at,
            deliver_at=cancelled_at,
        )
        self._pending_events.append(
            _PendingEventItem(
                deliver_at=cancelled_at,
                seq=self._next_seq(),
                event=cancel_evt,
            )
        )
        return True

    def poll_events(self, now: float) -> list[ProviderEvent]:
        """Poll events with deliver_at <= now, respecting outage delays/drops."""
        remaining: list[_PendingEventItem] = []
        ready: list[_PendingEventItem] = []

        for item in self._pending_events:
            # Check if deliver_at is impacted by any outage
            dropped = False
            for o in self._outages:
                if o.start <= item.deliver_at < o.end:
                    if o.events == "drop":
                        dropped = True
                        break
                    elif o.events == "delay":
                        item.deliver_at = max(item.deliver_at, o.end)

            if dropped:
                continue

            if item.deliver_at <= now:
                ready.append(item)
            else:
                remaining.append(item)

        self._pending_events = remaining
        ready.sort(key=lambda x: (x.deliver_at, x.seq))
        return [x.event for x in ready]

    def get_call_status(self, call_id: str, now: float) -> str | None:
        """Ground truth at provider computed from true occurred_at times."""
        if call_id not in self._call_records:
            return None

        record = self._call_records[call_id]

        if record.cancelled_at is not None and now >= record.cancelled_at:
            return "CANCELLED"

        if record.outcome.answers:
            if record.completed_at is not None and now >= record.completed_at:
                return "COMPLETED"
            if record.answered_at is not None and now >= record.answered_at:
                return "ANSWERED"
            if now >= record.ringing_at:
                return "RINGING"
            return "INITIATED"
        else:
            if record.failed_at is not None and now >= record.failed_at:
                return "FAILED"
            if now >= record.ringing_at:
                return "RINGING"
            return "INITIATED"

