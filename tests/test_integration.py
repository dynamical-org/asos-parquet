"""Integration tests that validate data after actual ingest.

These tests require network access and perform real data fetching.
Run with: pytest tests/test_integration.py -v
"""

import pytest

import pandas as pd

from asos_parquet.fetch import fetch_observations_batch, fetch_station_observations
from asos_parquet.load import observations_to_geoparquet
from asos_parquet.partitioned import DEFAULT_DATASET_PATH
from asos_parquet.stations import fetch_all_stations, fetch_network_stations
from asos_parquet.validation import validate_geoparquet


class TestStationFetching:
    """Test station metadata fetching."""

    def test_fetch_single_state(self):
        """Fetch stations for a single state."""
        stations = fetch_network_stations("CA")

        assert len(stations) > 0
        assert "station" in stations.columns
        assert "longitude" in stations.columns
        assert "latitude" in stations.columns
        assert "archive_begin" in stations.columns

    def test_fetch_all_states(self):
        """Fetch stations for multiple states."""
        stations = fetch_all_stations(states=["CA", "NY"])

        assert len(stations) > 0
        assert set(stations["state"].unique()) == {"CA", "NY"}

    def test_online_only_filter(self):
        """Test filtering for online stations only."""
        all_stations = fetch_all_stations(states=["CA"])
        online_stations = fetch_all_stations(states=["CA"], online_only=True)

        # Online stations should be subset
        assert len(online_stations) <= len(all_stations)
        assert all(online_stations["online"])


class TestObservationFetching:
    """Test observation data fetching."""

    def test_fetch_single_station(self):
        """Fetch observations for a single station."""
        start = pd.Timestamp("2024-12-01", tz="UTC")
        end = pd.Timestamp("2024-12-02", tz="UTC")

        df = fetch_station_observations("KSFO", start, end, state="CA")

        assert df is not None
        assert len(df) > 0
        assert "station" in df.columns
        assert "valid" in df.columns
        assert "tmpf" in df.columns
        assert "tmpc" in df.columns

    def test_fetch_batch(self):
        """Fetch observations for multiple stations."""
        stations = fetch_network_stations("CA").head(3)
        start = pd.Timestamp("2024-12-01", tz="UTC")
        end = pd.Timestamp("2024-12-02", tz="UTC")

        df = fetch_observations_batch(stations, start, end, show_progress=False)

        assert len(df) > 0
        assert "station" in df.columns
        assert df["station"].nunique() >= 1


class TestGeoparquetCreation:
    """Test geoparquet creation and validation."""

    def test_create_valid_geoparquet(self, tmp_path):
        """Create a geoparquet from fetched data and validate it."""
        # Fetch a small amount of data
        stations = fetch_network_stations("CA").head(5)
        start = pd.Timestamp("2024-12-01", tz="UTC")
        end = pd.Timestamp("2024-12-02", tz="UTC")

        df = fetch_observations_batch(stations, start, end, show_progress=False)

        # Convert to GeoDataFrame
        gdf = observations_to_geoparquet(df)

        # Write to file
        path = tmp_path / "test.parquet"
        gdf.to_parquet(path, compression="zstd", index=False)

        # Validate
        report = validate_geoparquet(path)

        print(report)  # For debugging

        # Core validations should pass
        schema_result = next(r for r in report.results if r.name == "schema_columns")
        geometry_result = next(r for r in report.results if r.name == "geometry")
        timestamps_result = next(r for r in report.results if r.name == "timestamps")

        assert schema_result.passed, f"Schema validation failed: {schema_result.message}"
        assert geometry_result.passed, f"Geometry validation failed: {geometry_result.message}"
        assert timestamps_result.passed, f"Timestamp validation failed: {timestamps_result.message}"


@pytest.mark.skipif(
    not (DEFAULT_DATASET_PATH / "year=2024" / "data.parquet").exists(),
    reason="No existing parquet data to validate",
)
class TestExistingData:
    """Tests that validate an existing geoparquet file."""

    def test_validate_existing_file(self):
        """Validate the existing geoparquet file."""
        path = DEFAULT_DATASET_PATH / "year=2024" / "data.parquet"
        report = validate_geoparquet(path)

        print(report)

        # All validations should pass
        assert report.passed, f"Validation failed:\n{report}"
