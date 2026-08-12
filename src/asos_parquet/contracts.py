from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class Variable(StrEnum):
    AIR_TEMPERATURE = "air_temperature"
    DEW_POINT = "dew_point"
    RELATIVE_HUMIDITY = "relative_humidity"
    PRECIPITATION_AMOUNT = "precipitation_amount"
    PRECIPITATION_TYPE = "precipitation_type"
    PRESENT_WEATHER = "present_weather"
    WIND_DIRECTION = "wind_direction"
    WIND_SPEED = "wind_speed"
    WIND_GUST = "wind_gust"


class ObservationStatistic(StrEnum):
    INSTANTANEOUS = "instantaneous"
    MEAN = "mean"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    SUM = "sum"


class ObservationQuality(StrEnum):
    ACCEPTED = "accepted"
    SUSPECT = "suspect"
    REJECTED = "rejected"


class ValueState(StrEnum):
    OBSERVED = "observed"
    MISSING = "missing"
    UNAVAILABLE = "unavailable"


class StationMatchMethod(StrEnum):
    EXACT = "exact"
    UNMATCHED = "unmatched"


def _assert_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp must be UTC-aware, got {value!r}")


def _require(value: bool, message: str) -> None:
    if not value:
        raise ValueError(message)


@dataclass(frozen=True, slots=True)
class RawObjectRef:
    source: str
    uri: str
    sha256: str
    ingested_at: datetime
    source_published_at: datetime | None = None

    def __post_init__(self) -> None:
        _require(bool(self.source), "Raw object source must not be empty")
        _require(bool(self.uri), "Raw object URI must not be empty")
        _require(
            len(self.sha256) == 64
            and all(character in "0123456789abcdef" for character in self.sha256),
            f"Raw object sha256 must be 64 lowercase hexadecimal characters: {self.sha256!r}",
        )
        _assert_utc(self.ingested_at)
        if self.source_published_at is not None:
            _assert_utc(self.source_published_at)


@dataclass(frozen=True, slots=True)
class NormalizedObservation:
    station_id: str
    source_station_id: str
    source_record_id: str
    revision_id: str
    supersedes_revision_id: str | None
    variable: Variable
    value: float | str | None
    unit: str
    observed_at: datetime
    available_at: datetime
    period: timedelta | None
    statistic: ObservationStatistic
    quality: ObservationQuality
    source_quality: str | None
    is_trace: bool
    raw: RawObjectRef
    value_state: ValueState = ValueState.OBSERVED

    def __post_init__(self) -> None:
        _require(bool(self.station_id), "Canonical station ID must not be empty")
        _require(bool(self.source_station_id), "Source station ID must not be empty")
        _require(bool(self.source_record_id), "Source record ID must not be empty")
        _require(bool(self.revision_id), "Revision ID must not be empty")
        _require(
            self.supersedes_revision_id != self.revision_id,
            "An observation revision cannot supersede itself",
        )
        _require(bool(self.unit), "Unit must not be empty")
        _assert_utc(self.observed_at)
        _assert_utc(self.available_at)
        if self.observed_at > self.available_at and self.quality is ObservationQuality.ACCEPTED:
            raise ValueError(
                f"Observation {self.station_id} {self.variable} at {self.observed_at!r} "
                f"is after availability {self.available_at!r} with accepted quality"
            )
        _require(
            self.available_at <= self.raw.ingested_at,
            f"Availability {self.available_at!r} is after raw ingest {self.raw.ingested_at!r}",
        )
        if self.period is not None:
            _require(self.period > timedelta(0), f"Period must be positive, got {self.period!r}")

        if self.value_state is ValueState.OBSERVED:
            _require(self.value is not None, "Observed values must not be null")
        else:
            _require(self.value is None, f"{self.value_state} values must be null")
            _require(not self.is_trace, f"{self.value_state} values cannot be trace observations")


@dataclass(frozen=True, slots=True)
class StationMapping:
    source: str
    source_station_id: str
    canonical_station_id: str | None
    method: StationMatchMethod
    valid_from: datetime
    valid_to: datetime | None

    def __post_init__(self) -> None:
        _require(bool(self.source), "Station mapping source must not be empty")
        _require(
            bool(self.source_station_id), "Station mapping source station ID must not be empty"
        )
        _assert_utc(self.valid_from)
        if self.valid_to is not None:
            _assert_utc(self.valid_to)
            _require(
                self.valid_from < self.valid_to,
                "Station mapping validity interval must be positive",
            )
        if self.method is StationMatchMethod.EXACT:
            _require(
                bool(self.canonical_station_id),
                "Exact station mappings require a canonical station ID",
            )
        else:
            _require(
                self.canonical_station_id is None,
                "Unmatched stations cannot have a canonical station ID",
            )


@dataclass(frozen=True, slots=True)
class Attribution:
    source: str
    title: str
    url: str
    license_name: str
    license_url: str

    def __post_init__(self) -> None:
        _require(
            all((self.source, self.title, self.url, self.license_name, self.license_url)),
            "Attribution fields must not be empty",
        )


@dataclass(frozen=True, slots=True)
class RawObjectManifest:
    raw: RawObjectRef
    size_bytes: int
    media_type: str
    attribution_source: str

    def __post_init__(self) -> None:
        _require(self.size_bytes >= 0, "Raw object size must not be negative")
        _require(bool(self.media_type), "Raw object media type must not be empty")
        _require(
            self.attribution_source == self.raw.source,
            "Raw object attribution source must match its source",
        )
