import logging

from asos_parquet import obs


def _reset_root() -> list[logging.Handler]:
    root = logging.getLogger()
    if hasattr(root, "_asos_configured"):
        delattr(root, "_asos_configured")
    original = root.handlers[:]
    root.handlers = []
    return original


def _restore_root(original: list[logging.Handler]) -> None:
    root = logging.getLogger()
    root.handlers = original
    if hasattr(root, "_asos_configured"):
        delattr(root, "_asos_configured")


def test_setup_logging_is_stream_only():
    root = logging.getLogger()
    original = _reset_root()
    try:
        obs.setup_logging()
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

        # Idempotent: a second call adds nothing.
        count = len(root.handlers)
        obs.setup_logging()
        assert len(root.handlers) == count
    finally:
        _restore_root(original)


def test_init_sentry_without_dsn_is_noop(monkeypatch):
    import sentry_sdk

    monkeypatch.delenv("SENTRY_DSN", raising=False)
    obs.init_sentry()  # must not raise
    assert not sentry_sdk.get_client().is_active()
