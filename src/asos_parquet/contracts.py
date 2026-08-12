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
    assert value.tzinfo is not None
    assert value.utcoffset() == timedelta(0)


@dataclass(frozen=True, slots=True)
class RawObjectRef:
    source: str
    uri: str
    sha256: str
    ingested_at: datetime
    source_published_at: datetime | None = None

    def __post_init__(self) -> None:
        assert self.source
        assert self.uri
        assert len(self.sha256) == 64
        assert all(character in "0123456789abcdef" for character in self.sha256)
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
        assert self.station_id
        assert self.source_station_id
        assert self.source_record_id
        assert self.revision_id
        assert self.unit
        _assert_utc(self.observed_at)
        _assert_utc(self.available_at)
        assert self.observed_at <= self.available_at
        assert self.available_at <= self.raw.ingested_at
        if self.period is not None:
            assert self.period > timedelta(0)
