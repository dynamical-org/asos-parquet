"""Opt-in bounded retries for IEM requests made by the scheduled bounded updater.

Callers that pass no policy keep their legacy retry loops unchanged. With a
policy, each operation (one network's metadata GET, one bulk chunk's GET) gets
a fixed number of attempts with short jittered backoff.

A shared monotonic ``Deadline`` bounds retries and backoff only. Every first
attempt runs, with full socket timeouts, even when the deadline passed while
it was queued; a first attempt that fails after the deadline is logged and
raised without a retry or a sleep. Retries must be admitted (paced, checked,
and given socket timeouts clamped to the remaining budget) before the
deadline, which is never renewed.

Socket timeouts are inactivity timeouts, not total-transfer limits: a request
that keeps dribbling bytes can outlive both its read timeout and the deadline,
so the caller's hard ceiling (Modal's function timeout) still applies.
"""

import http.client
import logging
import random
import ssl
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Protocol, TypeVar

import requests

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Retry/backoff budget for one scheduled bounded update, measured from entry.
# First attempts are not bounded by it.
SCHEDULED_ADMISSION_BUDGET = 600.0

# Transport failures worth another attempt. requests.ConnectionError covers
# SSLError too, so transient TLS failures (EOF, reset handshakes) are retried;
# certificate-verification failures are excluded in classify().
RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
    ConnectionResetError,
    TimeoutError,
)


class DeadlineExceeded(RuntimeError):
    """Raised instead of admitting a retry (or the wait before it) past the deadline."""


class RetryExhausted(RuntimeError):
    """Raised when every allowed attempt failed with a retryable error."""


class RetryableResponse(Exception):
    """A 200 response whose body is unusable now but may be fine on retry."""


class Pacer(Protocol):
    def wait(self, deadline: "Deadline | None" = None) -> None: ...


@dataclass(frozen=True)
class Deadline:
    """An absolute ``time.monotonic()`` instant after which no retry is admitted."""

    expires_at: float

    @classmethod
    def after(cls, seconds: float) -> "Deadline":
        return cls(time.monotonic() + seconds)

    def remaining(self) -> float:
        return self.expires_at - time.monotonic()

    def expired(self) -> bool:
        return self.remaining() <= 0

    def check(self, what: str = "request") -> None:
        if self.expired():
            raise DeadlineExceeded(f"admission deadline passed before {what}")


@dataclass(frozen=True)
class RetryPolicy:
    """Attempt limit, backoff, timeouts and deadline for one kind of operation."""

    deadline: Deadline
    max_attempts: int
    transient_delays: tuple[float, ...]  # delay before retry n, for transport/5xx/bad bodies
    throttle_delays: tuple[float, ...]  # delay before retry n, for 429
    connect_timeout: float
    read_timeout: float
    pace_first_attempt: bool  # False: only retries go through the pacer
    jitter: Callable[[], float] = random.random  # uniform 0–1 s added to each delay

    def first_timeout(self) -> tuple[float, float]:
        """(connect, read) socket timeouts for a first attempt: never clamped."""
        return (self.connect_timeout, self.read_timeout)

    def timeout(self) -> tuple[float, float]:
        """(connect, read) socket timeouts for a retry, clamped to the remaining budget."""
        remaining = self.deadline.remaining()
        if remaining <= 0:
            raise DeadlineExceeded("admission deadline passed before the request")
        return (min(self.connect_timeout, remaining), min(self.read_timeout, remaining))


def metadata_retry_policy(deadline: Deadline) -> RetryPolicy:
    """Station-metadata GETs: 3 attempts; first attempts unpaced, retries paced."""
    return RetryPolicy(
        deadline=deadline,
        max_attempts=3,
        transient_delays=(2.0, 4.0),
        throttle_delays=(6.0, 12.0),
        connect_timeout=5.0,
        read_timeout=30.0,
        pace_first_attempt=False,
    )


def bulk_retry_policy(deadline: Deadline) -> RetryPolicy:
    """Bulk observation GETs: 6 attempts, every attempt paced (first ones without deadline)."""
    return RetryPolicy(
        deadline=deadline,
        max_attempts=6,
        transient_delays=(2.0, 4.0, 8.0, 10.0, 10.0),
        throttle_delays=(6.0, 12.0, 24.0, 48.0, 96.0),
        connect_timeout=5.0,
        read_timeout=300.0,
        pace_first_attempt=True,
    )


def retry_after_seconds(response: requests.Response | None) -> float | None:
    """Seconds a 429/503 asks us to wait (delta-seconds or HTTP-date), if it says."""
    value = response.headers.get("Retry-After") if response is not None else None
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


# Upper bound on exceptions inspected when searching an error's wrapper graph.
_MAX_LINKED_ERRORS = 32


def _linked_errors(error: BaseException) -> list[BaseException]:
    """Exceptions this one wraps: cause, context, urllib3 ``reason`` and exception args."""
    linked = [error.__cause__, error.__context__, getattr(error, "reason", None), *error.args]
    return [e for e in linked if isinstance(e, BaseException)]


def _is_certificate_error(error: BaseException) -> bool:
    """True if a real ssl.SSLCertVerificationError is anywhere in the wrapper graph."""
    pending, seen = [error], set()
    while pending and len(seen) < _MAX_LINKED_ERRORS:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        pending.extend(_linked_errors(current))
    return False


def classify(error: BaseException) -> tuple[str, str] | None:
    """("throttle" | "transient", description) for a retryable error, else None."""
    if _is_certificate_error(error):
        return None
    if isinstance(error, requests.HTTPError):
        # A Response is falsy for 4xx/5xx, so test identity, not truthiness.
        status = error.response.status_code if error.response is not None else None
        if status == 429:
            return ("throttle", "HTTP 429")
        if status is None or status >= 500:
            return ("transient", f"HTTP {status}")
        return None
    if isinstance(error, RetryableResponse):
        return ("transient", str(error))
    if isinstance(error, RETRYABLE_EXCEPTIONS):
        return ("transient", type(error).__name__)
    return None


def _describe(error: BaseException) -> str:
    """A permanent failure's error class or HTTP status: never a URL, header or body."""
    if isinstance(error, requests.HTTPError):
        status = error.response.status_code if error.response is not None else None
        return f"HTTP {status}"
    return type(error).__name__


def call_with_retry(
    operation: Callable[[tuple[float, float]], T],
    *,
    label: str,
    policy: RetryPolicy,
    pacer: Pacer,
) -> T:
    """Run ``operation(timeout)`` under ``policy``; permanent errors propagate as-is.

    The first attempt always runs with full socket timeouts (paced without a
    deadline if ``pace_first_attempt``). Raises RetryExhausted after
    ``max_attempts`` retryable failures, and DeadlineExceeded when a retry (or
    the wait before it) cannot start before the deadline. Both chain the last
    underlying error.
    """
    started = time.monotonic()
    limit = policy.max_attempts
    first_error: str | None = None
    last_error: BaseException | None = None
    last_description: str | None = None

    for attempt in range(1, limit + 1):
        if attempt == 1:
            if policy.pace_first_attempt:
                pacer.wait(deadline=None)
            timeout = policy.first_timeout()
        else:
            try:
                what = f"attempt {attempt}/{limit}"
                policy.deadline.check(what)
                pacer.wait(deadline=policy.deadline)
                policy.deadline.check(what)
                timeout = policy.timeout()
            except DeadlineExceeded as e:
                message = f"{e}" + (
                    f" (last error: {last_description})" if last_description else ""
                )
                logger.warning(f"{label}: {message}", extra={"retry_event": "deadline"})
                raise DeadlineExceeded(message) from last_error

        try:
            result = operation(timeout)
        except Exception as error:
            classified = classify(error)
            elapsed = time.monotonic() - started
            if classified is None:
                logger.warning(
                    f"{label}: attempt {attempt}/{limit} failed ({_describe(error)}) "
                    f"after {elapsed:.1f}s; not retryable",
                    extra={"retry_event": "permanent"},
                )
                raise
            kind, description = classified
            first_error = first_error or description
            last_error, last_description = error, description
            failure = (
                f"{label}: attempt {attempt}/{limit} failed ({description}) after {elapsed:.1f}s"
            )

            if attempt == limit:
                logger.warning(f"{failure}; giving up", extra={"retry_event": "exhausted"})
                raise RetryExhausted(f"{description} after {attempt} attempts") from error

            delays = policy.throttle_delays if kind == "throttle" else policy.transient_delays
            delay = delays[attempt - 1] + policy.jitter()
            retry_after = None
            if isinstance(error, requests.HTTPError) and description in ("HTTP 429", "HTTP 503"):
                retry_after = retry_after_seconds(error.response)
                if retry_after is not None:
                    delay = max(delay, retry_after)
            note = f", Retry-After {retry_after:.0f}s" if retry_after is not None else ""

            remaining = policy.deadline.remaining()
            if delay >= remaining:
                message = (
                    f"{description}; retry {attempt}/{limit - 1} needs {delay:.1f}s{note} "
                    f"but {max(remaining, 0.0):.1f}s of admission budget remain"
                )
                logger.warning(f"{failure}; {message}", extra={"retry_event": "deadline"})
                raise DeadlineExceeded(message) from error

            logger.warning(
                f"{failure}; retry {attempt}/{limit - 1} in {delay:.1f}s{note}",
                extra={"retry_event": "retry"},
            )
            time.sleep(delay)
            continue

        if attempt > 1:
            logger.info(
                f"{label}: recovered on attempt {attempt}/{limit} after "
                f"{time.monotonic() - started:.1f}s (first error: {first_error})",
                extra={"retry_event": "recovered"},
            )
        return result

    raise AssertionError("unreachable")  # pragma: no cover
