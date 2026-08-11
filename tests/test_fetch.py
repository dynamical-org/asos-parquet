from pathlib import Path

import pandas as pd
import pytest

from asos_parquet.fetch import fetch_bulk_chunk, fetch_station_observations

FIXTURES = Path(__file__).parent / "fixtures"


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


@pytest.fixture
def iem_response(monkeypatch: pytest.MonkeyPatch) -> None:
    text = (FIXTURES / "iem_observations.csv").read_text()
    monkeypatch.setattr(
        "asos_parquet.fetch.requests.get",
        lambda *args, **kwargs: FakeResponse(text),
    )


def assert_existing_iem_eligibility(df: pd.DataFrame) -> None:
    assert len(df) == 4
    assert df["tmpf"].notna().all()
    assert (df["valid"] == pd.Timestamp("2026-08-01 01:00", tz="UTC")).sum() == 2
    trace = df.loc[df["valid"] == pd.Timestamp("2026-08-01 02:00", tz="UTC")].iloc[0]
    assert pd.isna(trace["p01i"])
    assert pd.isna(trace["p01m"])
    assert trace["wxcodes"] == "-RA"


def test_station_fetch_preserves_existing_iem_eligibility(iem_response: None) -> None:
    df = fetch_station_observations(
        "KJFK",
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
        state="NY",
    )

    assert df is not None
    assert_existing_iem_eligibility(df)
    assert set(df["state"]) == {"NY"}


def test_bulk_fetch_preserves_existing_iem_eligibility(iem_response: None) -> None:
    chunk_id, df, error = fetch_bulk_chunk(
        ["KJFK"],
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
        chunk_id=7,
    )

    assert chunk_id == 7
    assert error is None
    assert df is not None
    assert_existing_iem_eligibility(df)


def test_international_false_zero_evidence_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = (FIXTURES / "iem_international_false_zero.csv").read_text()
    monkeypatch.setattr(
        "asos_parquet.fetch.requests.get",
        lambda *args, **kwargs: FakeResponse(text),
    )

    df = fetch_station_observations(
        "ENBR",
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
    )

    assert df is not None
    assert df["p01m"].eq(0).all()
    assert set(df["wxcodes"]) == {"RA", "+RA", "VCSH"}
