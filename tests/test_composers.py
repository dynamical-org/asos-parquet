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
    # year is carried by the Hive directory name, not by a column in the file, so
    # reading the file back yields exactly the composed frame.
    stored = gpd.read_parquet(path)
    assert_frame_equal(stored, observations)


_METADATA_COLUMNS = ["name", "elevation", "country", "county", "wfo", "tzname"]


def _two_station_observations(*, temperature: float, hour: int) -> pd.DataFrame:
    kaaa = _observations(temperature=temperature)
    kbbb = _observations(temperature=temperature).assign(station="KBBB")
    both = pd.concat([kaaa, kbbb], ignore_index=True)
    return both.assign(valid=pd.Timestamp(f"2026-08-13T{hour:02d}:00:00Z"))


def _two_station_table() -> pd.DataFrame:
    kbbb = pd.DataFrame(
        {
            "station": ["KBBB"],
            "name": ["Old Name"],
            "elevation": [10.0],
            "country": ["US"],
            "county": ["Palm Beach"],
            "wfo": ["MFL"],
            "tzname": ["America/New_York"],
        }
    )
    return pd.concat([_stations(), kbbb], ignore_index=True)


def _existing_two_station_partition() -> gpd.GeoDataFrame:
    observations = _two_station_observations(temperature=60.0, hour=0).drop(columns="wxcodes")
    enriched = enrich_with_station_metadata(
        merge_observations(None, observations), _two_station_table()
    )
    return gpd.GeoDataFrame(enriched, geometry="geometry", crs="EPSG:4326")


def test_asos_composer_keeps_metadata_for_station_absent_from_fresh_table() -> None:
    existing = _existing_two_station_partition()
    # KBBB was re-keyed at IEM (like PBI -> DJT) so it no longer appears; KAAA's name changed.
    fresh = _stations().assign(name="Renamed")
    incoming = _observations(temperature=68.0).assign(valid=pd.Timestamp("2026-08-13T01:00:00Z"))

    actual = AsosParquetComposer().compose(existing, {"iem": SourceFrame("iem", incoming)}, fresh)

    kbbb = actual[actual["station"] == "KBBB"]
    assert len(kbbb) == 1
    assert kbbb[_METADATA_COLUMNS].iloc[0].tolist() == [
        "Old Name",
        10.0,
        "US",
        "Palm Beach",
        "MFL",
        "America/New_York",
    ]
    kaaa = actual[actual["station"] == "KAAA"]
    assert len(kaaa) == 2
    assert kaaa["name"].tolist() == ["Renamed", "Renamed"]
    assert kaaa["country"].tolist() == ["US", "US"]


def test_asos_composer_rejects_losing_country_for_present_station() -> None:
    existing = _existing_two_station_partition()
    fresh = _two_station_table()
    fresh.loc[fresh["station"] == "KAAA", "country"] = None

    with pytest.raises(ValueError, match="KAAA"):
        AsosParquetComposer().compose(
            existing,
            {"iem": SourceFrame("iem", _observations(temperature=68.0))},
            fresh,
        )


def test_asos_composer_leaves_new_rows_for_absent_station_null() -> None:
    existing = _existing_two_station_partition()
    incoming = _two_station_observations(temperature=68.0, hour=1)

    actual = AsosParquetComposer().compose(
        existing, {"iem": SourceFrame("iem", incoming)}, _stations()
    )

    kbbb = actual[actual["station"] == "KBBB"].set_index("valid")
    old = pd.Timestamp("2026-08-13T00:00:00Z")
    new = pd.Timestamp("2026-08-13T01:00:00Z")
    assert kbbb.loc[old, "country"] == "US"
    assert kbbb.loc[new, _METADATA_COLUMNS].isna().all()


def test_enrich_without_metadata_columns_matches_plain_merge() -> None:
    observations = merge_observations(
        None, _two_station_observations(temperature=60.0, hour=0).drop(columns="wxcodes")
    )
    stations = _stations()

    actual = enrich_with_station_metadata(observations, stations)

    expected = observations.merge(
        stations[["station", *_METADATA_COLUMNS]], on="station", how="left"
    )
    assert_frame_equal(actual, expected)


def test_asos_composer_tolerates_rows_already_missing_country() -> None:
    # Prod partitions already hold blanked rows for re-keyed stations (PBI, 2V5);
    # carrying those nulls forward is not a loss.
    existing = _existing_two_station_partition()
    existing.loc[existing["station"] == "KBBB", _METADATA_COLUMNS] = None

    actual = AsosParquetComposer().compose(
        existing,
        {"iem": SourceFrame("iem", _observations(temperature=68.0))},
        _stations(),
    )

    assert actual.loc[actual["station"] == "KBBB", "country"].isna().all()
