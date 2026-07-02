"""BetterStack observability: log streaming (Logtail) + error tracking (Sentry).

Both helpers degrade to no-ops when their BetterStack env vars are absent, so
local dev (`make load`) and the test suite run without touching the network. In
Modal, the env vars come from the `betterstack-asos-parquet` secret.
"""

from __future__ import annotations

import logging
import os

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

# Third-party loggers that emit a line per HTTP request / internal event at INFO.
# At our root level of INFO these would dominate Live tail (the fetch path makes
# thousands of requests per run), so we pin them to WARNING. Our own per-run
# summaries already capture the useful counts.
_NOISY_LOGGERS = ("httpx", "httpcore", "urllib3", "botocore", "boto3", "s3transfer")


def setup_logging() -> None:
    """Configure root logging once: always stream to stdout, and also stream to
    BetterStack when BETTERSTACK_SOURCE_TOKEN / _INGESTING_HOST are set.

    Idempotent — safe to call at the top of every Modal function invocation.
    """
    root = logging.getLogger()
    if getattr(root, "_asos_configured", False):
        return
    root._asos_configured = True  # type: ignore[attr-defined]

    root.setLevel(logging.INFO)
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    formatter = logging.Formatter(_LOG_FORMAT)

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    root.addHandler(stream)

    token = os.environ.get("BETTERSTACK_SOURCE_TOKEN")
    host = os.environ.get("BETTERSTACK_INGESTING_HOST")
    if token and host:
        from logtail import LogtailHandler

        root.addHandler(LogtailHandler(source_token=token, host=f"https://{host}"))


def init_sentry() -> None:
    """Initialize Sentry (BetterStack Errors) when BETTERSTACK_ERRORS_DSN is set.

    The logging integration turns ERROR-level log records (e.g. the
    `logger.exception(...)` in each Modal function's except block) into Sentry
    events.
    """
    dsn = os.environ.get("BETTERSTACK_ERRORS_DSN")
    if not dsn:
        return

    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_sdk.init(
        dsn=dsn,
        environment="production",
        traces_sample_rate=0.0,
        integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
    )


def flush() -> None:
    """Push buffered logs and error events before Modal freezes the container.

    Also stops Logtail's background FlushWorker. It is a non-daemon thread with
    no public stop API: it exits only after the thread that spawned it dies at
    interpreter shutdown, and under Modal the spawning thread is the container's
    main thread, which outlives every input. Left alone it lingers after the
    function returns, Modal logs a "background thread(s) still running after
    container exit" warning (which propagates from the modal-client logger back
    through this very handler into Better Stack), and container teardown stalls
    on the interpreter-shutdown join. `handler.flush()` has already drained the
    queue synchronously, so stopping the worker loses nothing — the shutdown
    drain it exists for is redundant here — and the handler re-spawns it on the
    next emit(), so a reused warm container keeps streaming.
    """
    import sentry_sdk

    sentry_sdk.flush()
    for handler in logging.getLogger().handlers:
        handler.flush()
        worker = getattr(handler, "flush_thread", None)  # Logtail's FlushWorker
        if worker is not None and worker.is_alive():
            worker.should_run = False
            # The worker re-checks should_run each step; a step lasts at most
            # flush_interval (1s) once the queue is drained.
            worker.join(timeout=2)
