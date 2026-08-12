from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

from asos_parquet.adapters.eccc_climate import normalize_observations
from asos_parquet.contracts import RawObjectRef, ValueState, Variable

FIXTURE = Path(__file__).parent / "fixtures" / "eccc_climate_hourly.json"


def _raw() -> RawObjectRef:
    data = FIXTURE.read_bytes()
    return RawObjectRef(
        source="eccc-climate-hourly",
        uri="https://api.weather.gc.ca/collections/climate-hourly/items?offset=0&limit=2",
        sha256=sha256(data).hexdigest(),
        ingested_at=datetime(2026, 8, 12, 12, tzinfo=UTC),
    )


def test_normalizes_variables_independently_and_preserves_flags() -> None:
    observations = normalize_observations(FIXTURE.read_bytes(), _raw())
    first = [item for item in observations if item.source_station_id == "8300590"]

    assert {item.variable for item in first} == {
        Variable.AIR_TEMPERATURE,
        Variable.DEW_POINT,
        Variable.RELATIVE_HUMIDITY,
        Variable.PRECIPITATION_AMOUNT,
        Variable.PRESENT_WEATHER,
        Variable.WIND_DIRECTION,
        Variable.WIND_SPEED,
    }
    precipitation = next(item for item in first if item.variable is Variable.PRECIPITATION_AMOUNT)
    assert precipitation.value == 0.4
    assert precipitation.period == timedelta(hours=1)
    assert precipitation.source_quality == "E"
    direction = next(item for item in first if item.variable is Variable.WIND_DIRECTION)
    assert direction.value == 260.0


def test_missing_precipitation_is_explicitly_unavailable_not_zero() -> None:
    observations = normalize_observations(FIXTURE.read_bytes(), _raw())
    precipitation = next(
        item
        for item in observations
        if item.source_station_id == "1108395" and item.variable is Variable.PRECIPITATION_AMOUNT
    )

    assert precipitation.value is None
    assert precipitation.value_state is ValueState.UNAVAILABLE


def test_rejects_non_eccc_raw_objects() -> None:
    raw = _raw()
    wrong = RawObjectRef(
        source="iem",
        uri=raw.uri,
        sha256=raw.sha256,
        ingested_at=raw.ingested_at,
    )

    try:
        normalize_observations(FIXTURE.read_bytes(), wrong)
    except ValueError as error:
        assert "eccc-climate-hourly" in str(error)
    else:
        raise AssertionError("Expected a source validation error")
