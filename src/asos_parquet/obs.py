"""Observability: stdout logging + Sentry (errors, logs, cron check-ins).

`init_sentry()` degrades to a no-op when SENTRY_DSN is absent, so local dev
(`make load`) and the test suite run without touching the network. In Modal,
the DSN comes from the `sentry-asos-parquet` secret.
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
    """Configure root logging once: stream to stdout, INFO and up.

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


def init_sentry() -> None:
    """Initialize Sentry (errors + logs) when SENTRY_DSN is set.

    `enable_logs` streams all log records to Sentry Logs; the logging
    integration additionally turns ERROR-level records (e.g. the
    `logger.exception(...)` in each Modal function's except block) into
    Sentry error events.
    """
    dsn = os.environ.get("SENTRY_DSN")
    if not dsn:
        return

    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_sdk.init(
        dsn=dsn,
        environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
        traces_sample_rate=0.0,
        enable_logs=True,
        integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
    )


def flush() -> None:
    """Push buffered Sentry logs and error events before Modal freezes the container."""
    import sentry_sdk

    sentry_sdk.flush()
    for handler in logging.getLogger().handlers:
        handler.flush()
