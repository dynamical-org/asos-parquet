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
    value: float | str
    unit: str
    observed_at: datetime
    available_at: datetime
    period: timedelta | None
    statistic: ObservationStatistic
    quality: ObservationQuality
    source_quality: str | None
    is_trace: bool
    raw: RawObjectRef

    def __post_init__(self) -> None:
        _require(bool(self.station_id), "Canonical station ID must not be empty")
        _require(bool(self.source_station_id), "Source station ID must not be empty")
        _require(bool(self.source_record_id), "Source record ID must not be empty")
        _require(bool(self.revision_id), "Revision ID must not be empty")
        _require(bool(self.unit), "Unit must not be empty")
        _assert_utc(self.observed_at)
        _assert_utc(self.available_at)
        if self.observed_at > self.available_at and self.quality is not ObservationQuality.SUSPECT:
            raise ValueError(
                f"Observation {self.station_id} {self.variable} at {self.observed_at!r} "
                f"is after availability {self.available_at!r} without suspect quality"
            )
        _require(
            self.available_at <= self.raw.ingested_at,
            f"Availability {self.available_at!r} is after raw ingest {self.raw.ingested_at!r}",
        )
        if self.period is not None:
            _require(self.period > timedelta(0), f"Period must be positive, got {self.period!r}")
