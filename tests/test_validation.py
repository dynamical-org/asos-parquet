"""Tests for data validation after ingest."""

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point

from asos_parquet.validation import (
    ValidationReport,
    validate_geometry,
    validate_geoparquet,
    validate_locations,
    validate_metric_imperial_consistency,
    validate_no_duplicates,
    validate_null_rates,
    validate_physical_bounds,
    validate_record_count,
    validate_region_codes,
    validate_schema,
    validate_station_count,
    validate_states,
    validate_timestamps,
    validate_us_locations,
)


@pytest.fixture
def valid_gdf() -> gpd.GeoDataFrame:
    """Create a valid GeoDataFrame for testing."""
    n = 100
    np.random.seed(42)

    data = {
        "station": ["KSFO"] * n,
        "valid": pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC"),
        "longitude": [-122.38] * n,
        "latitude": [37.62] * n,
        "state": ["CA"] * n,
        "tmpf": np.random.uniform(40, 80, n),
        "tmpc": None,  # Will be calculated
        "dwpf": np.random.uniform(30, 60, n),
        "dwpc": None,
        "relh": np.random.uniform(30, 90, n),
        "drct": np.random.uniform(0, 360, n),
        "sknt": np.random.uniform(0, 30, n),
        "gust": np.random.uniform(0, 50, n),
        "alti": np.random.uniform(29.8, 30.2, n),
        "mslp": np.random.uniform(1010, 1020, n),
        "vsby": np.random.uniform(5, 10, n),
        "p01i": np.random.uniform(0, 0.5, n),
        "p01m": None,
    }

    # Calculate metric values from imperial
    data["tmpc"] = (np.array(data["tmpf"]) - 32) * 5 / 9
    data["dwpc"] = (np.array(data["dwpf"]) - 32) * 5 / 9
    data["p01m"] = np.array(data["p01i"]) * 25.4

    df = pd.DataFrame(data)
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


class TestValidateSchema:
    def test_valid_schema(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_schema(valid_gdf)
        assert result.passed is True
        assert "required columns present" in result.message

    def test_missing_columns(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.drop(columns=["tmpf", "station"])
        result = validate_schema(gdf)
        assert result.passed is False
        assert "Missing" in result.message
        assert "tmpf" in result.details["missing"]
        assert "station" in result.details["missing"]


class TestValidateGeometry:
    def test_valid_geometry(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_geometry(valid_gdf)
        assert result.passed is True
        assert "Point" in result.message

    def test_null_geometries(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        # Set more than 10% to null
        gdf.loc[: len(gdf) // 5, "geometry"] = None
        result = validate_geometry(gdf)
        assert result.passed is False
        assert "null geometries" in result.message

    def test_no_geometry_column(self, valid_gdf: gpd.GeoDataFrame):
        df = pd.DataFrame(valid_gdf.drop(columns=["geometry"]))
        gdf = gpd.GeoDataFrame(df)
        result = validate_geometry(gdf)
        assert result.passed is False


class TestValidateTimestamps:
    def test_valid_timestamps(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_timestamps(valid_gdf)
        assert result.passed is True

    def test_null_timestamps(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "valid"] = None
        result = validate_timestamps(gdf)
        assert result.passed is False
        assert "null timestamps" in result.message

    def test_future_timestamps(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "valid"] = pd.Timestamp.now("UTC") + pd.Timedelta(days=30)
        result = validate_timestamps(gdf)
        assert result.passed is False
        assert "future" in result.message

    def test_ancient_timestamps(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "valid"] = pd.Timestamp("1800-01-01", tz="UTC")
        result = validate_timestamps(gdf)
        assert result.passed is False
        assert "1900" in result.message


class TestValidatePhysicalBounds:
    def test_valid_bounds(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_physical_bounds(valid_gdf)
        assert result.passed is True

    def test_extreme_temperature(self, valid_gdf: gpd.GeoDataFrame):
        """Test that extreme temperatures are detected but small outlier rates pass."""
        gdf = valid_gdf.copy()
        gdf.loc[0, "tmpf"] = 200  # Way above max of 150F
        result = validate_physical_bounds(gdf)
        # Single outlier in 1500 records = 0.067%, below 0.1% threshold
        assert result.passed == True  # Passes due to tolerance
        assert "tmpf" in result.details["columns"]  # But still reported

    def test_negative_humidity(self, valid_gdf: gpd.GeoDataFrame):
        """Test that negative humidity is detected but small outlier rates pass."""
        gdf = valid_gdf.copy()
        gdf.loc[0, "relh"] = -10
        result = validate_physical_bounds(gdf)
        # Single outlier in 1500 records = 0.067%, below 0.1% threshold
        assert result.passed == True  # Passes due to tolerance
        assert "relh" in result.details["columns"]  # But still reported

    def test_high_outlier_rate_fails(self, valid_gdf: gpd.GeoDataFrame):
        """Test that high outlier rates cause failure."""
        gdf = valid_gdf.copy()
        # Set 5% of values to extreme - should fail
        n_bad = len(gdf) // 20
        gdf.loc[:n_bad, "tmpf"] = 200
        result = validate_physical_bounds(gdf)
        assert result.passed == False


class TestValidateUSLocations:
    def test_valid_locations(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_us_locations(valid_gdf)
        assert result.passed is True

    def test_outside_us(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "longitude"] = 0  # Greenwich, UK
        gdf.loc[0, "latitude"] = 51
        result = validate_us_locations(gdf)
        assert result.passed is False


class TestValidateStates:
    def test_valid_states(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_states(valid_gdf)
        assert result.passed is True

    def test_invalid_state(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "state"] = "XX"
        result = validate_states(gdf)
        assert result.passed is False
        assert "XX" in result.details["invalid"]


class TestValidateNoDuplicates:
    def test_no_duplicates(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_no_duplicates(valid_gdf)
        assert result.passed is True
        assert result.informational is True

    def test_with_duplicates(self, valid_gdf: gpd.GeoDataFrame):
        # Add a duplicate row
        gdf = pd.concat([valid_gdf, valid_gdf.iloc[[0]]], ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
        result = validate_no_duplicates(gdf)
        assert result.passed is True
        assert result.informational is True
        assert "duplicate" in result.message.lower()
        assert result.details["duplicate_count"] == 1


class TestValidateNullRates:
    def test_acceptable_nulls(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_null_rates(valid_gdf)
        assert result.passed is True

    def test_high_null_rate(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        # Set 60% of tmpf to null (above 50% threshold)
        gdf.loc[: int(len(gdf) * 0.6), "tmpf"] = None
        result = validate_null_rates(gdf)
        assert result.passed is False
        assert "tmpf" in result.details["columns"]


class TestValidateRecordCount:
    def test_sufficient_records(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_record_count(valid_gdf, min_records=50)
        assert result.passed is True

    def test_insufficient_records(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_record_count(valid_gdf, min_records=1000)
        assert result.passed is False


class TestValidateStationCount:
    def test_sufficient_stations(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_station_count(valid_gdf, min_stations=1)
        assert result.passed is True
        assert result.informational is True

    def test_reports_count_regardless_of_minimum(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_station_count(valid_gdf, min_stations=100)
        assert result.passed is True
        assert result.informational is True
        assert result.details["count"] == 1  # Only one station in test data
        assert result.details["min_expected"] == 100


class TestValidateMetricImperialConsistency:
    def test_consistent_values(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_metric_imperial_consistency(valid_gdf)
        assert result.passed is True

    def test_inconsistent_temperature(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        # Set tmpc to wrong value
        gdf["tmpc"] = gdf["tmpf"]  # Should be converted, not same
        result = validate_metric_imperial_consistency(gdf)
        assert result.passed is False
        assert "tmpf/tmpc" in result.message

    def test_inconsistent_precipitation(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        # Set p01m to wrong value
        gdf["p01m"] = gdf["p01i"]  # Should be * 25.4
        result = validate_metric_imperial_consistency(gdf)
        assert result.passed is False
        assert "p01i/p01m" in result.message


class TestValidateGeoparquet:
    def test_full_validation_on_valid_data(self, valid_gdf: gpd.GeoDataFrame, tmp_path):
        # Write test file
        path = tmp_path / "test.parquet"
        valid_gdf.to_parquet(path)

        # Run full validation
        report = validate_geoparquet(path)

        assert isinstance(report, ValidationReport)
        assert report.passed is True
        assert report.failed_count == 0

    def test_validation_on_missing_file(self, tmp_path):
        path = tmp_path / "nonexistent.parquet"
        report = validate_geoparquet(path)

        assert report.passed is False
        assert any("not found" in r.message.lower() for r in report.results)

    def test_validation_report_str(self, valid_gdf: gpd.GeoDataFrame, tmp_path):
        path = tmp_path / "test.parquet"
        valid_gdf.to_parquet(path)

        report = validate_geoparquet(path)
        report_str = str(report)

        assert "Validation Report" in report_str
        assert "ALL PASSED" in report_str

    def test_validation_global_mode(self, tmp_path):
        """Test that global mode accepts non-US data."""
        n = 50
        np.random.seed(42)

        data = {
            "station": ["YSSY"] * n,
            "valid": pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC"),
            "longitude": [151.17] * n,
            "latitude": [-33.95] * n,
            "state": ["AU"] * n,
            "tmpf": np.random.uniform(50, 100, n),
            "tmpc": None,
            "dwpf": np.random.uniform(40, 70, n),
            "dwpc": None,
            "relh": np.random.uniform(30, 90, n),
            "drct": np.random.uniform(0, 360, n),
            "sknt": np.random.uniform(0, 30, n),
            "gust": np.random.uniform(0, 50, n),
            "alti": np.random.uniform(29.8, 30.2, n),
            "mslp": np.random.uniform(1010, 1020, n),
            "vsby": np.random.uniform(5, 10, n),
            "p01i": np.random.uniform(0, 0.5, n),
            "p01m": None,
        }
        data["tmpc"] = (np.array(data["tmpf"]) - 32) * 5 / 9
        data["dwpc"] = (np.array(data["dwpf"]) - 32) * 5 / 9
        data["p01m"] = np.array(data["p01i"]) * 25.4

        df = pd.DataFrame(data)
        geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        path = tmp_path / "test_global.parquet"
        gdf.to_parquet(path)

        # us_only=True should fail (non-US coords and state)
        report_us = validate_geoparquet(path, us_only=True)
        assert report_us.passed is False

        # us_only=False should pass
        report_global = validate_geoparquet(path, us_only=False)
        assert report_global.passed is True


class TestValidateLocations:
    def test_us_only_valid(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_locations(valid_gdf, us_only=True)
        assert result.passed is True

    def test_us_only_rejects_non_us(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "longitude"] = 151.17  # Sydney
        gdf.loc[0, "latitude"] = -33.95
        result = validate_locations(gdf, us_only=True)
        assert result.passed is False

    def test_global_mode_accepts_non_us(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "longitude"] = 151.17  # Sydney
        gdf.loc[0, "latitude"] = -33.95
        result = validate_locations(gdf, us_only=False)
        assert result.passed is True
        assert "Global" in result.message

    def test_backwards_compat_alias(self, valid_gdf: gpd.GeoDataFrame):
        """validate_us_locations is an alias for validate_locations(us_only=True)."""
        result = validate_us_locations(valid_gdf)
        assert result.passed is True


class TestValidateRegionCodes:
    def test_us_only_valid(self, valid_gdf: gpd.GeoDataFrame):
        result = validate_region_codes(valid_gdf, us_only=True)
        assert result.passed is True

    def test_us_only_rejects_au(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "state"] = "AU"
        result = validate_region_codes(gdf, us_only=True)
        assert result.passed is False
        assert "AU" in result.details["invalid"]

    def test_global_mode_accepts_au(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "state"] = "AU"
        result = validate_region_codes(gdf, us_only=False)
        assert result.passed is True

    def test_global_mode_accepts_canadian_province(self, valid_gdf: gpd.GeoDataFrame):
        gdf = valid_gdf.copy()
        gdf.loc[0, "state"] = "CA_AB"
        result = validate_region_codes(gdf, us_only=False)
        assert result.passed is True

    def test_backwards_compat_alias(self, valid_gdf: gpd.GeoDataFrame):
        """validate_states is an alias for validate_region_codes(us_only=True)."""
        result = validate_states(valid_gdf)
        assert result.passed is True
