"""Integration tests that validate data after actual ingest.

These tests require network access and perform real data fetching.
Run with: pytest tests/test_integration.py -v
"""

import pandas as pd
import pytest

from asos_parquet.fetch import fetch_observations_batch, fetch_station_observations
from asos_parquet.load import observations_to_geoparquet
from asos_parquet.partitioned import DEFAULT_DATASET_PATH
from asos_parquet.stations import fetch_all_stations, fetch_network_stations
from asos_parquet.validation import validate_geoparquet


class TestStationFetching:
    """Test station metadata fetching."""

    def test_fetch_single_state(self):
        """Fetch stations for a single state."""
        stations = fetch_network_stations("CA_ASOS")

        assert len(stations) > 0
        assert "station" in stations.columns
        assert "longitude" in stations.columns
        assert "latitude" in stations.columns
        assert "archive_begin" in stations.columns

    def test_fetch_all_states(self):
        """Fetch stations for multiple states."""
        stations = fetch_all_stations(networks=["CA_ASOS", "NY_ASOS"])

        assert len(stations) > 0
        assert set(stations["state"].unique()) == {"CA", "NY"}

    def test_online_only_filter(self):
        """Test filtering for online stations only."""
        all_stations = fetch_all_stations(networks=["CA_ASOS"])
        online_stations = fetch_all_stations(networks=["CA_ASOS"], online_only=True)

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
        stations = fetch_network_stations("CA_ASOS").head(3)
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
        stations = fetch_network_stations("CA_ASOS").head(5)
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

    def test_no_mutation_of_input_dataframe(self):
        """Verify that observations_to_geoparquet doesn't mutate input DataFrame."""
        # Create a simple test DataFrame
        df = pd.DataFrame(
            {
                "station": [123, 456, 789],  # Numeric station IDs
                "valid": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03"], utc=True
                ),
                "longitude": [-122.0, -121.0, -120.0],
                "latitude": [37.0, 38.0, 39.0],
                "tmpf": [50.0, 55.0, 60.0],
            }
        )

        # Store original values for comparison
        original_station_values = df["station"].copy()
        original_station_dtype = df["station"].dtype
        original_index = df.index.copy()

        # Call the function
        gdf = observations_to_geoparquet(df)

        # Verify input DataFrame was NOT mutated
        assert df["station"].dtype == original_station_dtype, "Station dtype was mutated"
        assert df["station"].equals(original_station_values), "Station values were mutated"
        assert df.index.equals(original_index), "Index was mutated"

        # Verify output GeoDataFrame has correct transformations
        # Check that dtype is a string type (object or StringDtype)
        assert pd.api.types.is_string_dtype(gdf["station"]), "Output station should be string type"
        assert all(isinstance(x, str) for x in gdf["station"]), "All stations should be strings"
        assert len(gdf) == 3, "Output should have same number of rows"


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
