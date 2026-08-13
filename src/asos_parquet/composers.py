from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import geopandas as gpd
import pandas as pd

from .config import LEGACY_DATA_FIELDS
from .load import enrich_with_station_metadata, merge_observations, write_year_partition
from .validation import OBS_V1_REQUIRED_COLUMNS, REQUIRED_COLUMNS


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    name: str
    required_columns: tuple[str, ...]


asos_parquet_schema = DatasetSchema(
    name="asos-parquet",
    required_columns=tuple(REQUIRED_COLUMNS),
)
obs_schema = DatasetSchema(
    name="obs-parquet-v1",
    required_columns=tuple(OBS_V1_REQUIRED_COLUMNS),
)


@dataclass(frozen=True, slots=True)
class SourceFrame:
    source: str
    observations: pd.DataFrame


class Composer(Protocol):
    schema: DatasetSchema

    def compose(
        self,
        existing: gpd.GeoDataFrame | None,
        sources: Mapping[str, SourceFrame],
        stations: pd.DataFrame,
    ) -> gpd.GeoDataFrame: ...


class Publisher(Protocol):
    def publish(self, observations: gpd.GeoDataFrame, year: int) -> Path: ...


@dataclass(frozen=True, slots=True)
class ParquetPublisher:
    base_path: Path

    def publish(self, observations: gpd.GeoDataFrame, year: int) -> Path:
        return write_year_partition(observations, year, base_path=self.base_path)


@dataclass(frozen=True, slots=True)
class AsosParquetComposer:
    schema: DatasetSchema = asos_parquet_schema

    def compose(
        self,
        existing: gpd.GeoDataFrame | None,
        sources: Mapping[str, SourceFrame],
        stations: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
        if set(sources) != {"iem"}:
            raise ValueError("asos-parquet requires exactly the IEM source")
        source = sources["iem"]
        if source.source != "iem":
            raise ValueError(f"Expected IEM source frame, got {source.source!r}")
        observations = source.observations.drop(columns="wxcodes", errors="ignore")
        composed = merge_observations(existing, observations)
        enriched = enrich_with_station_metadata(composed, stations)
        missing = [column for column in self.schema.required_columns if column not in enriched]
        if missing:
            raise ValueError(f"asos-parquet composition is missing columns: {missing}")
        allowed = {
            "station",
            "valid",
            "longitude",
            "latitude",
            "state",
            "geometry",
            *LEGACY_DATA_FIELDS,
            "name",
            "elevation",
            "country",
            "county",
            "wfo",
            "tzname",
            "bbox",
            "year",
        }
        unexpected = sorted(set(enriched.columns) - allowed)
        if unexpected:
            raise ValueError(f"asos-parquet composition has unexpected columns: {unexpected}")
        return gpd.GeoDataFrame(enriched, geometry="geometry", crs="EPSG:4326")
