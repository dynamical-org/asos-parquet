from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import pytest

from asos_parquet.canonical import (
    normalized_partition,
    read_attributions,
    read_normalized,
    read_raw_manifests,
    read_station_mappings,
    select_canonical,
    write_attributions,
    write_normalized,
    write_raw_manifests,
    write_station_mappings,
)
from asos_parquet.contracts import (
    Attribution,
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectManifest,
    RawObjectRef,
    StationMapping,
    StationMatchMethod,
    ValueState,
    Variable,
)


def observation(
    source: str,
    revision: str,
    available_hour: int,
    value: float,
    supersedes: str | None = None,
    period: timedelta | None = None,
) -> NormalizedObservation:
    raw = RawObjectRef(
        source=source,
        uri=f"s3://raw/{source}/{revision}",
        sha256=revision[0] * 64,
        ingested_at=datetime(2026, 1, 1, available_hour, 5, tzinfo=UTC),
    )
    return NormalizedObservation(
        station_id="station:001",
        source_station_id="001",
        source_record_id=f"{source}:001:2026-01-01T00:00:00Z:temperature",
        revision_id=revision,
        supersedes_revision_id=supersedes,
        variable=Variable.AIR_TEMPERATURE,
        value=value,
        unit="degree_Celsius",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 1, 1, available_hour, tzinfo=UTC),
        period=period,
        statistic=ObservationStatistic.INSTANTANEOUS,
        quality=ObservationQuality.ACCEPTED,
        source_quality=None,
        is_trace=False,
        raw=raw,
    )


def test_source_representatives_round_trip_without_losing_semantics(tmp_path: Path) -> None:
    sources = ["iem", "msc-swob", "wis2"]
    observations = [
        observation(source, chr(ord("a") + index), index + 1, 10.0 + index)
        for index, source in enumerate(sources)
    ]
    path = tmp_path / "normalized.parquet"

    write_normalized(observations, path)
    restored = read_normalized(path)

    assert restored == observations


def test_empty_normalized_dataset_retains_readable_schema(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"

    write_normalized([], path)

    assert read_normalized(path) == []


def test_late_revision_does_not_change_earlier_as_of_snapshot() -> None:
    original = observation("iem", "a", 1, 10.0)
    correction = observation("iem", "b", 3, 11.0, supersedes="a")

    early = select_canonical(
        [original, correction],
        as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
        source_precedence={"iem": 0},
    )
    late = select_canonical(
        [original, correction],
        as_of=datetime(2026, 1, 1, 4, tzinfo=UTC),
        source_precedence={"iem": 0},
    )

    assert early == [original]
    assert late == [correction]


def test_source_precedence_is_deterministic() -> None:
    iem = observation("iem", "a", 1, 10.0)
    swob = observation("msc-swob", "b", 1, 11.0)

    selected = select_canonical(
        [iem, swob],
        as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
        source_precedence={"msc-swob": 0, "iem": 1},
    )

    assert selected == [swob]


def test_unknown_source_precedence_is_rejected() -> None:
    with pytest.raises(ValueError, match="Missing source precedence"):
        select_canonical(
            [observation("unknown", "a", 1, 10.0)],
            as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
            source_precedence={"iem": 0},
        )


def test_canonical_sort_handles_instantaneous_and_period_values() -> None:
    instantaneous = observation("iem", "a", 1, 10.0)
    period = observation("iem", "b", 1, 10.0, period=timedelta(hours=1))

    selected = select_canonical(
        [period, instantaneous],
        as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
        source_precedence={"iem": 0},
    )

    assert selected == [instantaneous, period]


def test_station_mapping_is_explicit() -> None:
    exact = StationMapping(
        source="msc-swob",
        source_station_id="CYUL",
        canonical_station_id="wigos:0-20000-0-71627",
        method=StationMatchMethod.EXACT,
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
        valid_to=None,
    )
    unmatched = StationMapping(
        source="wis2",
        source_station_id="unknown",
        canonical_station_id=None,
        method=StationMatchMethod.UNMATCHED,
        valid_from=datetime(2026, 1, 1, tzinfo=UTC),
        valid_to=None,
    )

    assert exact.canonical_station_id is not None
    assert unmatched.canonical_station_id is None


def test_value_states_distinguish_missing_unavailable_and_trace() -> None:
    base = observation("iem", "a", 1, 0.0)
    missing = NormalizedObservation(
        station_id=base.station_id,
        source_station_id=base.source_station_id,
        source_record_id="missing",
        revision_id="missing",
        supersedes_revision_id=None,
        variable=Variable.RELATIVE_HUMIDITY,
        value=None,
        unit="percent",
        observed_at=base.observed_at,
        available_at=base.available_at,
        period=None,
        statistic=ObservationStatistic.INSTANTANEOUS,
        quality=ObservationQuality.ACCEPTED,
        source_quality=None,
        is_trace=False,
        raw=base.raw,
        value_state=ValueState.MISSING,
    )
    unavailable = NormalizedObservation(
        station_id=base.station_id,
        source_station_id=base.source_station_id,
        source_record_id="unavailable",
        revision_id="unavailable",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_AMOUNT,
        value=None,
        unit="mm",
        observed_at=base.observed_at,
        available_at=base.available_at,
        period=timedelta(hours=1),
        statistic=ObservationStatistic.SUM,
        quality=ObservationQuality.ACCEPTED,
        source_quality=None,
        is_trace=False,
        raw=base.raw,
        value_state=ValueState.UNAVAILABLE,
    )
    trace = NormalizedObservation(
        station_id=base.station_id,
        source_station_id=base.source_station_id,
        source_record_id="trace",
        revision_id="trace",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_AMOUNT,
        value=0.0,
        unit="mm",
        observed_at=base.observed_at,
        available_at=base.available_at,
        period=timedelta(hours=1),
        statistic=ObservationStatistic.SUM,
        quality=ObservationQuality.SUSPECT,
        source_quality="T",
        is_trace=True,
        raw=base.raw,
    )

    assert missing.value_state is ValueState.MISSING
    assert unavailable.value_state is ValueState.UNAVAILABLE
    assert trace.value_state is ValueState.OBSERVED
    assert trace.is_trace is True


def test_normalized_partition_is_source_and_event_time_based() -> None:
    assert normalized_partition(observation("iem", "a", 1, 10.0)) == {
        "source": "iem",
        "year": 2026,
        "month": 1,
    }


def test_manifests_mappings_and_attributions_round_trip(tmp_path: Path) -> None:
    raw = RawObjectRef(
        source="msc-swob",
        uri="s3://raw/msc-swob/report.xml",
        sha256="c" * 64,
        ingested_at=datetime(2026, 1, 1, 1, tzinfo=UTC),
        source_published_at=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
    )
    manifests = [
        RawObjectManifest(
            raw=raw,
            size_bytes=1234,
            media_type="application/xml",
            attribution_source="msc-swob",
        )
    ]
    mappings = [
        StationMapping(
            source="msc-swob",
            source_station_id="CYUL",
            canonical_station_id=None,
            method=StationMatchMethod.UNMATCHED,
            valid_from=datetime(2026, 1, 1, tzinfo=UTC),
            valid_to=None,
        )
    ]
    attributions = [
        Attribution(
            source="msc-swob",
            title="Meteorological Service of Canada SWOB",
            url="https://dd.weather.gc.ca/",
            license_name="MSC Datamart terms",
            license_url="https://eccc-msc.github.io/open-data/licence/readme_en/",
        )
    ]

    write_raw_manifests(manifests, tmp_path / "raw.parquet")
    write_station_mappings(mappings, tmp_path / "stations.parquet")
    write_attributions(attributions, tmp_path / "attributions.parquet")

    assert read_raw_manifests(tmp_path / "raw.parquet") == manifests
    assert read_station_mappings(tmp_path / "stations.parquet") == mappings
    assert read_attributions(tmp_path / "attributions.parquet") == attributions


def test_numeric_values_are_coerced_or_rejected_before_writing(tmp_path: Path) -> None:
    integer_value = replace(observation("iem", "a", 1, 10.0), value=270)
    numpy_value = replace(observation("iem", "b", 1, 10.0), value=cast(Any, np.float32(12.5)))
    path = tmp_path / "numeric.parquet"

    write_normalized([integer_value, numpy_value], path)
    restored = read_normalized(path)

    assert [item.value for item in restored] == [270.0, 12.5]
    with pytest.raises(ValueError, match="finite"):
        write_normalized([replace(integer_value, value=float("nan"))], path)


def test_supersession_is_scoped_to_source_and_canonical_key() -> None:
    temperature = observation("msc-swob", "a", 1, 10.0)
    wind = replace(
        observation("wis2", "a", 1, 5.0),
        variable=Variable.WIND_SPEED,
        unit="m s-1",
    )
    wind_fix = replace(wind, revision_id="b", supersedes_revision_id="a", value=6.0)

    selected = select_canonical(
        [temperature, wind, wind_fix],
        as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
        source_precedence={"msc-swob": 0, "wis2": 1},
    )

    assert selected == [temperature, wind_fix]


def test_observed_accepted_value_beats_later_missing_or_suspect_record() -> None:
    accepted = observation("iem", "a", 1, 10.0)
    missing = replace(
        observation("iem", "b", 2, 11.0),
        value=None,
        value_state=ValueState.MISSING,
    )
    suspect = replace(observation("iem", "c", 3, 12.0), quality=ObservationQuality.SUSPECT)

    selected = select_canonical(
        [accepted, missing, suspect],
        as_of=datetime(2026, 1, 1, 4, tzinfo=UTC),
        source_precedence={"iem": 0},
    )

    assert selected == [accepted]


def test_write_normalized_sorts_for_predicate_pushdown(tmp_path: Path) -> None:
    earlier = observation("iem", "a", 1, 10.0)
    later_station = replace(earlier, station_id="station:999", revision_id="b")
    path = tmp_path / "sorted.parquet"

    write_normalized([later_station, earlier], path)

    assert list(pd.read_parquet(path)["station_id"]) == ["station:001", "station:999"]


def test_revision_cannot_supersede_itself() -> None:
    with pytest.raises(ValueError, match="cannot supersede itself"):
        replace(observation("iem", "a", 1, 10.0), supersedes_revision_id="a")


def test_observed_value_beats_higher_precedence_unavailable_value() -> None:
    unavailable = replace(
        observation("eccc-climate-hourly", "a", 1, 10.0),
        value=None,
        value_state=ValueState.UNAVAILABLE,
    )
    observed = observation("eccc-swob", "b", 1, 0.6)

    selected = select_canonical(
        [unavailable, observed],
        as_of=datetime(2026, 1, 1, 2, tzinfo=UTC),
        source_precedence={"eccc-climate-hourly": 0, "eccc-swob": 1},
    )

    assert selected == [observed]
