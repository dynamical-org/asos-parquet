from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from asos_parquet.composers import (
    AsosParquetComposer,
    ParquetPublisher,
    SourceFrame,
    asos_parquet_schema,
    obs_schema,
)
from asos_parquet.load import enrich_with_station_metadata, merge_observations


def _observations(*, temperature: float, wxcodes: str = "RA") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": ["KAAA"],
            "valid": [pd.Timestamp("2026-08-13T00:00:00Z")],
            "longitude": [-90.0],
            "latitude": [40.0],
            "state": ["IA"],
            "tmpf": [temperature],
            "tmpc": [(temperature - 32) * 5 / 9],
            "dwpf": [50.0],
            "dwpc": [10.0],
            "relh": [80.0],
            "drct": [180.0],
            "sknt": [10.0],
            "gust": [15.0],
            "alti": [29.92],
            "mslp": [1013.0],
            "vsby": [10.0],
            "p01i": [0.1],
            "p01m": [2.54],
            "wxcodes": [wxcodes],
        }
    )


def _stations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": ["KAAA"],
            "name": ["Example"],
            "elevation": [250.0],
            "country": ["US"],
            "county": ["Story"],
            "wfo": ["DMX"],
            "tzname": ["America/Chicago"],
        }
    )


def test_schema_names_are_dataset_contracts() -> None:
    assert asos_parquet_schema.name == "asos-parquet"
    assert "wxcodes" not in asos_parquet_schema.required_columns
    assert obs_schema.name == "obs-parquet-v1"
    assert "wxcodes" in obs_schema.required_columns


def test_asos_composer_matches_existing_merge_and_enrichment() -> None:
    existing = merge_observations(None, _observations(temperature=60.0).drop(columns="wxcodes"))
    incoming = _observations(temperature=68.0)
    expected = enrich_with_station_metadata(
        merge_observations(existing, incoming.drop(columns="wxcodes")), _stations()
    )

    actual = AsosParquetComposer().compose(
        existing,
        {"iem": SourceFrame("iem", incoming)},
        _stations(),
    )

    assert_frame_equal(actual, expected)
    assert "wxcodes" not in actual
    assert actual.iloc[0]["tmpf"] == 68.0


def test_asos_composer_rejects_other_source_sets() -> None:
    composer = AsosParquetComposer()

    with pytest.raises(ValueError, match="exactly the IEM source"):
        composer.compose(None, {}, _stations())
    with pytest.raises(ValueError, match="exactly the IEM source"):
        composer.compose(
            None,
            {
                "iem": SourceFrame("iem", _observations(temperature=68.0)),
                "eccc": SourceFrame("eccc", _observations(temperature=68.0)),
            },
            _stations(),
        )


def test_parquet_publisher_uses_explicit_destination(tmp_path: Path) -> None:
    observations = AsosParquetComposer().compose(
        None,
        {"iem": SourceFrame("iem", _observations(temperature=68.0))},
        _stations(),
    )

    path = ParquetPublisher(tmp_path / "asos-parquet").publish(observations, 2026)

    assert path == tmp_path / "asos-parquet" / "year=2026" / "data.parquet"
    stored = gpd.read_parquet(path)
    assert_frame_equal(stored.drop(columns="year"), observations)
