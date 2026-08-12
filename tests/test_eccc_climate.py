from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path

import pytest

from asos_parquet.adapters.eccc_climate import normalize_observations
from asos_parquet.contracts import ObservationQuality, RawObjectRef, Variable

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


def test_missing_precipitation_is_omitted_not_zero() -> None:
    observations = normalize_observations(FIXTURE.read_bytes(), _raw())
    precipitation = [
        item
        for item in observations
        if item.source_station_id == "1108395" and item.variable is Variable.PRECIPITATION_AMOUNT
    ]

    assert precipitation == []


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


def _changed_feature(**properties: object) -> bytes:
    document = loads(FIXTURE.read_bytes())
    document["features"] = [deepcopy(document["features"][0])]
    document["features"][0]["properties"].update(properties)
    return dumps(document).encode()


def test_blank_numeric_value_is_omitted_and_invalid_value_has_context() -> None:
    blank = normalize_observations(_changed_feature(TEMP=""), _raw())
    assert not any(item.variable is Variable.AIR_TEMPERATURE for item in blank)

    with pytest.raises(ValueError, match=r"TEMP.*8300590\.2026\.8\.10\.0"):
        normalize_observations(_changed_feature(TEMP="not-a-number"), _raw())


@pytest.mark.parametrize("field", ["CLIMATE_IDENTIFIER", "ID"])
def test_missing_identity_is_rejected(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        normalize_observations(_changed_feature(**{field: None}), _raw())


def test_trace_future_and_out_of_bounds_values_are_classified() -> None:
    trace = normalize_observations(
        _changed_feature(PRECIP_AMOUNT=0.0, PRECIP_AMOUNT_FLAG="T"), _raw()
    )
    precipitation = next(item for item in trace if item.variable is Variable.PRECIPITATION_AMOUNT)
    assert precipitation.is_trace is True
    assert precipitation.value == 0.0

    future = normalize_observations(
        _changed_feature(UTC_DATE="2026-08-13T08:00:00", TEMP=-9999.9), _raw()
    )
    temperature = next(item for item in future if item.variable is Variable.AIR_TEMPERATURE)
    assert temperature.quality is ObservationQuality.SUSPECT
    assert temperature.source_quality == "physical_bounds;future_observation"
