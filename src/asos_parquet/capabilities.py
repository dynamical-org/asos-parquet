from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import (
    CapabilityState,
    NormalizedObservation,
    ObservationQuality,
    StationVariableCapability,
    ValueState,
    Variable,
)


def _day_start(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=UTC)


def _state(
    expected_count: int,
    observed_count: int,
    accepted_count: int,
    accepted_ratio_threshold: float,
) -> tuple[CapabilityState, str]:
    if observed_count == 0:
        return CapabilityState.ABSENT, "no_observations"
    if accepted_count / expected_count < accepted_ratio_threshold:
        return CapabilityState.DEGRADED, "accepted_coverage_below_threshold"
    return CapabilityState.PRESENT, "accepted_coverage_meets_threshold"


def derive_daily_capabilities(
    observations: Iterable[NormalizedObservation],
    variables: Sequence[Variable],
    accepted_ratio_threshold: float = 0.8,
) -> list[StationVariableCapability]:
    if not 0 < accepted_ratio_threshold <= 1:
        raise ValueError("accepted_ratio_threshold must be in (0, 1]")
    records = list(observations)
    if not records:
        return []

    expected: dict[tuple[str, str, date], set[datetime]] = defaultdict(set)
    observed: dict[tuple[str, str, date, Variable], set[str]] = defaultdict(set)
    accepted: dict[tuple[str, str, date, Variable], set[str]] = defaultdict(set)
    precipitation_contradictions: dict[tuple[str, str, date], set[str]] = defaultdict(set)
    station_days: dict[tuple[str, str], set[date]] = defaultdict(set)
    for item in records:
        source = item.raw.source
        day = item.observed_at.date()
        station_key = (source, item.source_station_id)
        station_days[station_key].add(day)
        expected[(source, item.source_station_id, day)].add(item.observed_at)
        key = (source, item.source_station_id, day, item.variable)
        if item.value_state is ValueState.OBSERVED:
            observed[key].add(item.source_record_id)
            if item.quality is ObservationQuality.ACCEPTED:
                accepted[key].add(item.source_record_id)
            if (
                item.variable is Variable.PRECIPITATION_AMOUNT
                and item.source_quality is not None
                and "present_weather_contradicts_zero" in item.source_quality
            ):
                precipitation_contradictions[(source, item.source_station_id, day)].add(
                    item.source_record_id
                )

    daily: list[StationVariableCapability] = []
    for (source, station_id), days in sorted(station_days.items()):
        day = min(days)
        while day <= max(days):
            expected_count = len(expected[(source, station_id, day)])
            if expected_count:
                for variable in variables:
                    key = (source, station_id, day, variable)
                    observed_count = min(len(observed[key]), expected_count)
                    accepted_count = min(len(accepted[key]), observed_count)
                    state, reason = _state(
                        expected_count,
                        observed_count,
                        accepted_count,
                        accepted_ratio_threshold,
                    )
                    if (
                        variable is Variable.PRECIPITATION_AMOUNT
                        and precipitation_contradictions[(source, station_id, day)]
                    ):
                        state = CapabilityState.DEGRADED
                        reason = "present_weather_contradicts_zero"
                    daily.append(
                        StationVariableCapability(
                            source=source,
                            source_station_id=station_id,
                            variable=variable,
                            state=state,
                            valid_from=_day_start(day),
                            valid_to=_day_start(day + timedelta(days=1)),
                            expected_count=expected_count,
                            observed_count=observed_count,
                            accepted_count=accepted_count,
                            reason=reason,
                        )
                    )
            day += timedelta(days=1)
    return _compress(daily)


def _compress(
    capabilities: Sequence[StationVariableCapability],
) -> list[StationVariableCapability]:
    compressed: list[StationVariableCapability] = []
    ordered = sorted(
        capabilities,
        key=lambda item: (
            item.source,
            item.source_station_id,
            str(item.variable),
            item.valid_from,
        ),
    )
    for item in ordered:
        if compressed:
            previous = compressed[-1]
            if (
                previous.source == item.source
                and previous.source_station_id == item.source_station_id
                and previous.variable is item.variable
                and previous.state is item.state
                and previous.reason == item.reason
                and previous.valid_to == item.valid_from
            ):
                compressed[-1] = StationVariableCapability(
                    source=previous.source,
                    source_station_id=previous.source_station_id,
                    variable=previous.variable,
                    state=previous.state,
                    valid_from=previous.valid_from,
                    valid_to=item.valid_to,
                    expected_count=previous.expected_count + item.expected_count,
                    observed_count=previous.observed_count + item.observed_count,
                    accepted_count=previous.accepted_count + item.accepted_count,
                    reason=previous.reason,
                )
                continue
        compressed.append(item)
    return compressed


CAPABILITY_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        ("source", pa.string()),
        ("source_station_id", pa.string()),
        ("variable", pa.string()),
        ("state", pa.string()),
        ("valid_from", pa.timestamp("us", tz="UTC")),
        ("valid_to", pa.timestamp("us", tz="UTC")),
        ("expected_count", pa.int64()),
        ("observed_count", pa.int64()),
        ("accepted_count", pa.int64()),
        ("reason", pa.string()),
    ]
)


def write_capabilities(capabilities: Iterable[StationVariableCapability], path: Path) -> None:
    rows = [
        {
            "source": item.source,
            "source_station_id": item.source_station_id,
            "variable": str(item.variable),
            "state": str(item.state),
            "valid_from": item.valid_from,
            "valid_to": item.valid_to,
            "expected_count": item.expected_count,
            "observed_count": item.observed_count,
            "accepted_count": item.accepted_count,
            "reason": item.reason,
        }
        for item in capabilities
    ]
    if rows:
        table = pa.Table.from_pandas(
            pd.DataFrame(rows), schema=CAPABILITY_SCHEMA, preserve_index=False
        )
    else:
        table = pa.Table.from_batches([], schema=CAPABILITY_SCHEMA)
    pq.write_table(table, path, compression="zstd")


def read_capabilities(path: Path) -> list[StationVariableCapability]:
    return [
        StationVariableCapability(
            source=str(row["source"]),
            source_station_id=str(row["source_station_id"]),
            variable=Variable(str(row["variable"])),
            state=CapabilityState(str(row["state"])),
            valid_from=pd.Timestamp(row["valid_from"]).to_pydatetime(),
            valid_to=pd.Timestamp(row["valid_to"]).to_pydatetime(),
            expected_count=int(row["expected_count"]),
            observed_count=int(row["observed_count"]),
            accepted_count=int(row["accepted_count"]),
            reason=str(row["reason"]),
        )
        for row in pd.read_parquet(path).to_dict("records")
    ]
