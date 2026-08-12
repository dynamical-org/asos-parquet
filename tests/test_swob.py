from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

from asos_parquet.adapters.swob import normalize_swob
from asos_parquet.contracts import ObservationQuality, RawObjectRef, Variable

FIXTURES = Path(__file__).parent / "fixtures"


def _raw(data: bytes) -> RawObjectRef:
    return RawObjectRef(
        source="msc-swob",
        uri="https://dd.weather.gc.ca/example.xml",
        sha256=sha256(data).hexdigest(),
        ingested_at=datetime(2026, 8, 12, 19, tzinfo=UTC),
    )


def test_normalizes_core_fields_with_periods_and_provenance() -> None:
    data = (FIXTURES / "swob_core.xml").read_bytes()
    observations = normalize_swob(data, _raw(data))
    by_variable = {item.variable: item for item in observations}

    assert set(by_variable) == {
        Variable.AIR_TEMPERATURE,
        Variable.DEW_POINT,
        Variable.RELATIVE_HUMIDITY,
        Variable.PRECIPITATION_AMOUNT,
        Variable.PRESENT_WEATHER,
        Variable.WIND_SPEED,
        Variable.WIND_DIRECTION,
        Variable.WIND_GUST,
    }
    assert by_variable[Variable.PRECIPITATION_AMOUNT].period == timedelta(hours=1)
    assert by_variable[Variable.WIND_SPEED].period == timedelta(minutes=10)
    assert by_variable[Variable.PRESENT_WEATHER].value == "RA"
    assert all(item.raw == _raw(data) for item in observations)
    assert all(
        item.available_at == datetime(2026, 8, 12, 18, 2, tzinfo=UTC) for item in observations
    )


def test_normalizes_partner_identity_and_marks_nonpassing_qa_suspect() -> None:
    data = (
        (FIXTURES / "swob_partner.xml")
        .read_bytes()
        .replace(
            b'value="100"/></element><element name="rel_hum"',
            b'value="80"/></element><element name="rel_hum"',
            1,
        )
    )
    observations = normalize_swob(data, _raw(data))

    assert {item.source_station_id for item in observations} == {"ON-MNRF-AFFES_GAL"}
    temperature = next(item for item in observations if item.variable is Variable.AIR_TEMPERATURE)
    assert temperature.quality is ObservationQuality.SUSPECT
    assert temperature.source_quality == "qa_summary:80"


def test_missing_airport_precipitation_is_not_synthetic_zero() -> None:
    data = (FIXTURES / "swob_airport_missing_precip.xml").read_bytes()
    observations = normalize_swob(data, _raw(data))

    assert [item.variable for item in observations] == [Variable.AIR_TEMPERATURE]
