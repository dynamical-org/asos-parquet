"""Data validation for ASOS geoparquet.

Provides validation checks to ensure data correctness after ingest.
"""

from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas as pd

from .config import ALL_COUNTRIES, CANADIAN_PROVINCES, LEGACY_DATA_FIELDS, US_STATES

# All valid region codes (US states + Canadian provinces + international country codes)
ALL_REGION_CODES = set(US_STATES) | set(CANADIAN_PROVINCES) | set(ALL_COUNTRIES)
# Canadian provinces use "CA_XX" format in the state column
ALL_REGION_CODES.update(f"CA_{prov}" for prov in CANADIAN_PROVINCES)


@dataclass
class ValidationResult:
    """Result of a validation check.

    Attributes:
        name: Check name (e.g., "schema_columns")
        passed: Whether the check passed
        message: Human-readable result message
        details: Additional structured data
        informational: If True, this check is for metrics only and
            doesn't affect the overall pass/fail status
    """

    name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)
    informational: bool = False

    def __str__(self) -> str:
        if self.informational:
            return f"[INFO] {self.name}: {self.message}"
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.message}"


@dataclass
class ValidationReport:
    """Complete validation report for a geoparquet file."""

    path: str
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Check if all non-informational validations passed."""
        return all(r.passed for r in self.results if not r.informational)

    @property
    def failed_count(self) -> int:
        """Count non-informational failures only."""
        return sum(1 for r in self.results if not r.passed and not r.informational)

    @property
    def informational_results(self) -> list[ValidationResult]:
        """Get informational (metrics-only) results."""
        return [r for r in self.results if r.informational]

    def __str__(self) -> str:
        lines = [
            f"Validation Report: {self.path}",
            "=" * 60,
        ]
        for result in self.results:
            lines.append(str(result))
        lines.append("=" * 60)
        status = "ALL PASSED" if self.passed else f"{self.failed_count} FAILED"
        lines.append(f"Summary: {len(self.results)} checks, {status}")
        return "\n".join(lines)


# Expected columns in the geoparquet
REQUIRED_COLUMNS = [
    "station",
    "valid",
    "longitude",
    "latitude",
    "state",
    "geometry",
] + LEGACY_DATA_FIELDS
OBS_V1_REQUIRED_COLUMNS = [*REQUIRED_COLUMNS, "wxcodes"]

# Physical bounds for weather variables
PHYSICAL_BOUNDS = {
    "tmpf": (-130.0, 150.0),  # Temperature (F): -130 to 150
    "tmpc": (-90.0, 65.0),  # Temperature (C): -90 to 65
    "dwpf": (-130.0, 100.0),  # Dew point (F)
    "dwpc": (-90.0, 40.0),  # Dew point (C)
    "relh": (0.0, 100.0),  # Relative humidity (%)
    "drct": (0.0, 360.0),  # Wind direction (degrees)
    "sknt": (0.0, 250.0),  # Wind speed (knots): max ~250 for extreme gusts
    "gust": (0.0, 300.0),  # Wind gust (knots)
    "alti": (25.0, 32.0),  # Altimeter (inches): ~25-32 for sea level
    "mslp": (850.0, 1100.0),  # Sea level pressure (mb)
    "vsby": (0.0, 100.0),  # Visibility (miles)
    "p01i": (0.0, 20.0),  # 1-hour precip (inches): max ~20 for extreme events
    "p01m": (0.0, 500.0),  # 1-hour precip (mm)
    "longitude": (-180.0, 180.0),
    "latitude": (-90.0, 90.0),
}

# US bounding box (approximate, includes territories)
# Note: Aleutian Islands cross the International Date Line, so some Alaska
# stations (e.g., PASY at Shemya AFB) have positive longitudes (~174°E)
US_BBOX = {
    "min_lon": -180.0,  # Western Alaska
    "max_lon": -65.0,  # East coast (but see aleutian_lon below)
    "min_lat": 17.0,  # Puerto Rico
    "max_lat": 72.0,  # Northern Alaska
    "aleutian_min_lon": 170.0,  # Far western Aleutians (positive longitude)
    "aleutian_max_lon": 180.0,
}


def validate_schema(
    gdf: gpd.GeoDataFrame, required_columns: list[str] = REQUIRED_COLUMNS
) -> ValidationResult:
    """Validate that all required columns exist."""
    missing = [col for col in required_columns if col not in gdf.columns]

    if missing:
        return ValidationResult(
            name="schema_columns",
            passed=False,
            message=f"Missing {len(missing)} required columns",
            details={"missing": missing},
        )

    return ValidationResult(
        name="schema_columns",
        passed=True,
        message=f"All {len(required_columns)} required columns present",
        details={"columns": list(gdf.columns)},
    )


def validate_geometry(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate geometry column."""
    if "geometry" not in gdf.columns:
        return ValidationResult(
            name="geometry",
            passed=False,
            message="No geometry column found",
        )

    # Check for null geometries
    null_geom = gdf.geometry.isna().sum()

    # Check geometry types
    geom_types = gdf.geometry.dropna().type.unique().tolist()

    # Check CRS
    crs = str(gdf.crs) if gdf.crs else "None"

    if null_geom > len(gdf) * 0.1:  # More than 10% null is concerning
        return ValidationResult(
            name="geometry",
            passed=False,
            message=f"{null_geom:,} null geometries ({null_geom / len(gdf) * 100:.1f}%)",
            details={"null_count": null_geom, "types": geom_types, "crs": crs},
        )

    if "Point" not in geom_types:
        return ValidationResult(
            name="geometry",
            passed=False,
            message=f"Expected Point geometry, found: {geom_types}",
            details={"types": geom_types, "crs": crs},
        )

    return ValidationResult(
        name="geometry",
        passed=True,
        message=f"Valid Point geometries, CRS: EPSG:4326",
        details={"null_count": null_geom, "types": geom_types, "crs": crs},
    )


def validate_timestamps(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate timestamp column."""
    if "valid" not in gdf.columns:
        return ValidationResult(
            name="timestamps",
            passed=False,
            message="No 'valid' timestamp column found",
        )

    valid_col = gdf["valid"]

    # Check if datetime type
    if not pd.api.types.is_datetime64_any_dtype(valid_col):
        return ValidationResult(
            name="timestamps",
            passed=False,
            message=f"'valid' column is not datetime type: {valid_col.dtype}",
        )

    # Check for null values
    null_count = valid_col.isna().sum()

    # Check timezone
    has_tz = hasattr(valid_col.dt, "tz") and valid_col.dt.tz is not None

    # Check date range (should be between 1900 and now+1day)
    min_date = valid_col.min()
    max_date = valid_col.max()
    now = pd.Timestamp.now("UTC")

    issues = []
    if null_count > 0:
        issues.append(f"{null_count:,} null timestamps")
    if min_date < pd.Timestamp("1900-01-01", tz="UTC"):
        issues.append(f"min date {min_date} before 1900")
    if max_date > now + pd.Timedelta(days=1):
        issues.append(f"max date {max_date} in the future")

    if issues:
        return ValidationResult(
            name="timestamps",
            passed=False,
            message="; ".join(issues),
            details={"min": str(min_date), "max": str(max_date), "null_count": null_count},
        )

    return ValidationResult(
        name="timestamps",
        passed=True,
        message=f"Valid range: {min_date.date()} to {max_date.date()}",
        details={
            "min": str(min_date),
            "max": str(max_date),
            "null_count": null_count,
            "has_tz": has_tz,
        },
    )


def validate_physical_bounds(
    gdf: gpd.GeoDataFrame,
    max_outlier_rate: float = 0.001,  # Allow up to 0.1% outliers
) -> ValidationResult:
    """Validate that weather values are within physical bounds.

    Args:
        gdf: GeoDataFrame with weather data
        max_outlier_rate: Maximum fraction of values allowed outside bounds (default 0.1%)
    """
    out_of_bounds = {}
    total_values = 0
    total_outliers = 0

    for col, (min_val, max_val) in PHYSICAL_BOUNDS.items():
        if col not in gdf.columns:
            continue

        values = gdf[col].dropna()
        if len(values) == 0:
            continue

        below = (values < min_val).sum()
        above = (values > max_val).sum()
        total_values += len(values)
        total_outliers += below + above

        if below > 0 or above > 0:
            out_of_bounds[col] = {
                "below_min": int(below),
                "above_max": int(above),
                "bounds": (min_val, max_val),
                "actual_min": float(values.min()),
                "actual_max": float(values.max()),
            }

    if out_of_bounds:
        total_issues = sum(v["below_min"] + v["above_max"] for v in out_of_bounds.values())
        outlier_rate = total_outliers / total_values if total_values > 0 else 0

        # Pass if outlier rate is below threshold
        passed = outlier_rate <= max_outlier_rate
        pct = outlier_rate * 100

        return ValidationResult(
            name="physical_bounds",
            passed=passed,
            message=f"{total_issues:,} values ({pct:.4f}%) outside bounds in {len(out_of_bounds)} columns",
            details={"columns": out_of_bounds, "outlier_rate": outlier_rate},
        )

    return ValidationResult(
        name="physical_bounds",
        passed=True,
        message="All values within physical bounds",
    )


def validate_locations(gdf: gpd.GeoDataFrame, us_only: bool = True) -> ValidationResult:
    """Validate that locations are within expected boundaries.

    Args:
        gdf: GeoDataFrame with longitude/latitude columns
        us_only: If True, enforce US bounding box. If False, report coordinate
            ranges (physical_bounds already validates valid lat/lon ranges).
    """
    if "longitude" not in gdf.columns or "latitude" not in gdf.columns:
        return ValidationResult(
            name="locations",
            passed=False,
            message="Missing longitude/latitude columns",
        )

    lon = gdf["longitude"].dropna()
    lat = gdf["latitude"].dropna()

    if not us_only:
        # Global mode: just report coordinate ranges (informational)
        return ValidationResult(
            name="locations",
            passed=True,
            message=(
                f"Global: {len(lon):,} locations, "
                f"lon [{float(lon.min()):.1f}, {float(lon.max()):.1f}], "
                f"lat [{float(lat.min()):.1f}, {float(lat.max()):.1f}]"
            ),
            details={
                "lon_range": (float(lon.min()), float(lon.max())),
                "lat_range": (float(lat.min()), float(lat.max())),
            },
        )

    # US-only mode: enforce US bounding box
    # Check latitude bounds
    outside_lat = ((lat < US_BBOX["min_lat"]) | (lat > US_BBOX["max_lat"])).sum()

    # Check longitude bounds - allow both Western Hemisphere AND Aleutian Islands
    # Western US: -180 to -65 (negative longitudes)
    # Aleutians: 170 to 180 (positive longitudes past date line)
    in_western_us = (lon >= US_BBOX["min_lon"]) & (lon <= US_BBOX["max_lon"])
    in_aleutians = (lon >= US_BBOX["aleutian_min_lon"]) & (lon <= US_BBOX["aleutian_max_lon"])
    outside_lon = (~(in_western_us | in_aleutians)).sum()

    if outside_lon > 0 or outside_lat > 0:
        return ValidationResult(
            name="locations",
            passed=False,
            message=f"{outside_lon + outside_lat:,} points outside US boundaries",
            details={
                "outside_lon": int(outside_lon),
                "outside_lat": int(outside_lat),
                "lon_range": (float(lon.min()), float(lon.max())),
                "lat_range": (float(lat.min()), float(lat.max())),
            },
        )

    return ValidationResult(
        name="locations",
        passed=True,
        message=f"All {len(lon):,} locations within US boundaries",
        details={
            "lon_range": (float(lon.min()), float(lon.max())),
            "lat_range": (float(lat.min()), float(lat.max())),
        },
    )


def validate_us_locations(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that locations are within US boundaries (backwards-compat alias)."""
    return validate_locations(gdf, us_only=True)


def validate_region_codes(gdf: gpd.GeoDataFrame, us_only: bool = True) -> ValidationResult:
    """Validate that region codes (state column) are recognized.

    Args:
        gdf: GeoDataFrame with state column
        us_only: If True, only accept US state codes. If False, accept all
            known region codes (US states, Canadian provinces, country codes).
    """
    if "state" not in gdf.columns:
        return ValidationResult(
            name="region_codes",
            passed=False,
            message="No 'state' column found",
        )

    codes = gdf["state"].dropna().unique()
    valid_set = set(US_STATES) if us_only else ALL_REGION_CODES
    invalid = [s for s in codes if s not in valid_set]

    if invalid:
        return ValidationResult(
            name="region_codes",
            passed=False,
            message=f"{len(invalid)} invalid region codes: {invalid}",
            details={"invalid": invalid, "valid_count": len(codes) - len(invalid)},
        )

    label = "state" if us_only else "region"
    return ValidationResult(
        name="region_codes",
        passed=True,
        message=f"All {len(codes)} {label} codes valid",
        details={"codes": sorted(codes.tolist())},
    )


def validate_states(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that state codes are valid US states (backwards-compat alias)."""
    return validate_region_codes(gdf, us_only=True)


def validate_no_duplicates(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Check for duplicate (station, valid) pairs (informational metric).

    ASOS data can have legitimate duplicates when stations issue corrections,
    special observations (SPECIs), or re-transmissions at the same timestamp.
    The Iowa Mesonet archives all of these, which is valuable for completeness.

    This is an informational check that reports duplicate counts but does not
    affect pass/fail status.
    """
    if "station" not in gdf.columns or "valid" not in gdf.columns:
        return ValidationResult(
            name="duplicates",
            passed=True,
            message="Missing station or valid column",
            informational=True,
        )

    duplicates = gdf.duplicated(subset=["station", "valid"]).sum()

    if duplicates > 0:
        dup_rate = duplicates / len(gdf) * 100
        return ValidationResult(
            name="duplicates",
            passed=True,  # Always passes (informational only)
            message=f"{duplicates:,} duplicate (station, valid) pairs ({dup_rate:.3f}%)",
            details={"duplicate_count": int(duplicates), "duplicate_rate": dup_rate},
            informational=True,
        )

    return ValidationResult(
        name="duplicates",
        passed=True,
        message="No duplicate (station, valid) pairs",
        informational=True,
    )


def validate_null_rates(
    gdf: gpd.GeoDataFrame,
    max_null_rate: float = 0.5,
) -> ValidationResult:
    """Validate that null rates are acceptable for key columns."""
    high_null = {}

    key_columns = ["station", "valid", "tmpf", "tmpc", "longitude", "latitude"]

    for col in key_columns:
        if col not in gdf.columns:
            continue

        null_rate = gdf[col].isna().sum() / len(gdf)
        if null_rate > max_null_rate:
            high_null[col] = round(null_rate * 100, 1)

    if high_null:
        return ValidationResult(
            name="null_rates",
            passed=False,
            message=f"{len(high_null)} columns exceed {max_null_rate * 100}% null rate",
            details={"columns": high_null},
        )

    return ValidationResult(
        name="null_rates",
        passed=True,
        message=f"All key columns below {max_null_rate * 100}% null rate",
    )


def validate_record_count(
    gdf: gpd.GeoDataFrame,
    min_records: int = 1,
) -> ValidationResult:
    """Validate minimum record count."""
    count = len(gdf)

    if count < min_records:
        return ValidationResult(
            name="record_count",
            passed=False,
            message=f"Only {count:,} records, expected at least {min_records:,}",
            details={"count": count, "min_required": min_records},
        )

    return ValidationResult(
        name="record_count",
        passed=True,
        message=f"{count:,} records",
        details={"count": count},
    )


def validate_station_count(
    gdf: gpd.GeoDataFrame,
    min_stations: int = 1,
) -> ValidationResult:
    """Report station count (informational metric).

    Historical data may have limited station counts due to archive availability.
    This is an informational check that reports the station count but does not
    affect pass/fail status.
    """
    if "station" not in gdf.columns:
        return ValidationResult(
            name="station_count",
            passed=True,
            message="No 'station' column found",
            informational=True,
        )

    count = gdf["station"].nunique()

    return ValidationResult(
        name="station_count",
        passed=True,  # Always passes (informational only)
        message=f"{count} unique stations",
        details={"count": count, "min_expected": min_stations},
        informational=True,
    )


def validate_station_coverage(
    gdf: gpd.GeoDataFrame,
    expected_stations: pd.DataFrame,
    year: int,
) -> ValidationResult:
    """Measure station coverage (informational metric).

    Compares stations present in the data against stations that should have
    been reporting during the given year (based on archive_begin metadata).

    This is an informational check that provides coverage metrics but does
    not affect pass/fail status. Historical data often has incomplete station
    coverage due to manual reporting or stations not yet existing.

    Args:
        gdf: GeoDataFrame with observation data
        expected_stations: DataFrame with station metadata (must have 'station' and 'archive_begin')
        year: The year being validated

    Returns:
        ValidationResult with coverage metrics (informational=True)
    """
    if "station" not in gdf.columns:
        return ValidationResult(
            name="station_coverage",
            passed=False,
            message="No 'station' column found",
            informational=True,
        )

    # Filter to stations that should have data for this year
    year_end = pd.Timestamp(f"{year}-12-31", tz="UTC")

    active_stations = expected_stations[(expected_stations["archive_begin"] <= year_end)][
        "station"
    ].unique()

    # Get stations actually present in data
    present_stations = set(gdf["station"].unique())
    expected_set = set(active_stations)

    missing = expected_set - present_stations
    extra = present_stations - expected_set  # Stations in data but not in metadata

    missing_pct = len(missing) / len(expected_set) * 100 if expected_set else 0
    coverage_pct = 100 - missing_pct

    msg = f"{len(present_stations)} of {len(expected_set)} stations ({coverage_pct:.1f}% coverage)"
    return ValidationResult(
        name="station_coverage",
        passed=True,  # Always passes (informational only)
        message=msg,
        details={
            "missing_stations": sorted(missing)[:50],  # Cap at 50 for readability
            "missing_count": len(missing),
            "missing_pct": round(missing_pct, 1),
            "expected_count": len(expected_set),
            "present_count": len(present_stations),
            "coverage_pct": round(coverage_pct, 1),
            "extra_count": len(extra),
        },
        informational=True,
    )


def validate_temporal_completeness(
    gdf: gpd.GeoDataFrame,
    max_gap_hours: int = 48,
    min_observations_per_day: int = 12,
) -> ValidationResult:
    """Measure temporal completeness (informational metric).

    Checks for stations with significant gaps or low observation counts.
    ASOS stations typically report hourly (24 obs/day), so fewer than
    min_observations_per_day suggests missing data.

    This is an informational check that provides completeness metrics but
    does not affect pass/fail status. Historical data often has sparse
    observations due to manual reporting or equipment limitations.

    Args:
        gdf: GeoDataFrame with observation data
        max_gap_hours: Gap threshold for flagging (hours)
        min_observations_per_day: Threshold for sparse stations

    Returns:
        ValidationResult with completeness metrics (informational=True)
    """
    if "station" not in gdf.columns or "valid" not in gdf.columns:
        return ValidationResult(
            name="temporal_completeness",
            passed=False,
            message="Missing 'station' or 'valid' column",
            informational=True,
        )

    # Calculate date range
    date_range = (gdf["valid"].max() - gdf["valid"].min()).days + 1
    if date_range <= 0:
        return ValidationResult(
            name="temporal_completeness",
            passed=True,
            message="Single day of data",
            informational=True,
        )

    # Check observation density per station
    station_stats = gdf.groupby("station").agg(
        obs_count=("valid", "count"),
        min_time=("valid", "min"),
        max_time=("valid", "max"),
    )

    # Calculate days covered and expected observations
    station_stats["days"] = (station_stats["max_time"] - station_stats["min_time"]).dt.days + 1
    station_stats["obs_per_day"] = station_stats["obs_count"] / station_stats["days"]

    # Find stations with low observation density
    sparse_stations = station_stats[station_stats["obs_per_day"] < min_observations_per_day]

    # Sample check for large gaps (expensive, so only check subset)
    large_gaps = []
    sample_stations = gdf["station"].unique()[:100]  # Check up to 100 stations

    for station in sample_stations:
        station_data = gdf[gdf["station"] == station].sort_values("valid")
        if len(station_data) < 2:
            continue

        time_diffs = station_data["valid"].diff().dropna()
        max_gap = time_diffs.max()

        if max_gap > pd.Timedelta(hours=max_gap_hours):
            large_gaps.append(
                {
                    "station": station,
                    "max_gap_hours": max_gap.total_seconds() / 3600,
                }
            )

    sparse_pct = len(sparse_stations) / len(station_stats) * 100 if len(station_stats) > 0 else 0
    dense_pct = 100 - sparse_pct
    median_obs = float(station_stats["obs_per_day"].median()) if len(station_stats) > 0 else 0

    return ValidationResult(
        name="temporal_completeness",
        passed=True,  # Always passes (informational only)
        message=f"Median {median_obs:.1f} obs/day; {dense_pct:.1f}% stations well-covered",
        details={
            "sparse_stations": sparse_stations.index.tolist()[:20],
            "sparse_count": len(sparse_stations),
            "sparse_pct": round(sparse_pct, 1),
            "dense_pct": round(dense_pct, 1),
            "large_gap_stations": large_gaps[:20],
            "large_gap_count": len(large_gaps),
            "total_stations": len(station_stats),
            "median_obs_per_day": median_obs,
        },
        informational=True,
    )


def validate_metric_imperial_consistency(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that metric and imperial values are consistent."""
    issues = []

    # Check temperature: tmpc should be (tmpf - 32) * 5/9
    if "tmpf" in gdf.columns and "tmpc" in gdf.columns:
        mask = gdf["tmpf"].notna() & gdf["tmpc"].notna()
        if mask.sum() > 0:
            expected_tmpc = (gdf.loc[mask, "tmpf"] - 32) * 5 / 9
            diff = (gdf.loc[mask, "tmpc"] - expected_tmpc).abs()
            bad = (diff > 0.5).sum()  # Allow 0.5C tolerance for rounding
            if bad > 0:
                issues.append(f"{bad:,} inconsistent tmpf/tmpc pairs")

    # Check precipitation: p01m should be p01i * 25.4
    if "p01i" in gdf.columns and "p01m" in gdf.columns:
        mask = gdf["p01i"].notna() & gdf["p01m"].notna()
        if mask.sum() > 0:
            expected_p01m = gdf.loc[mask, "p01i"] * 25.4
            diff = (gdf.loc[mask, "p01m"] - expected_p01m).abs()
            bad = (diff > 1.0).sum()  # Allow 1mm tolerance
            if bad > 0:
                issues.append(f"{bad:,} inconsistent p01i/p01m pairs")

    if issues:
        return ValidationResult(
            name="metric_imperial_consistency",
            passed=False,
            message="; ".join(issues),
        )

    return ValidationResult(
        name="metric_imperial_consistency",
        passed=True,
        message="Metric and imperial values are consistent",
    )


def validate_geoparquet(
    path: Path | str,
    min_records: int = 1,
    min_stations: int = 1,
    expected_stations: pd.DataFrame | None = None,
    year: int | None = None,
    us_only: bool = True,
    schema_version: str = "legacy",
) -> ValidationReport:
    """Run all validation checks on a geoparquet file.

    Args:
        path: Path to the geoparquet file
        min_records: Minimum expected record count
        min_stations: Minimum expected station count
        expected_stations: Optional station metadata for coverage validation
        year: Year of data (required if expected_stations provided)
        us_only: If True, enforce US-only location/state checks

    Returns:
        ValidationReport with all check results
    """
    path = Path(path)
    if schema_version not in {"legacy", "obs-v1"}:
        raise ValueError(f"Unknown schema version: {schema_version!r}")

    report = ValidationReport(path=str(path))

    # Check file exists
    if not path.exists():
        report.results.append(
            ValidationResult(
                name="file_exists",
                passed=False,
                message=f"File not found: {path}",
            )
        )
        return report

    # Load the file
    try:
        gdf = gpd.read_parquet(path)
    except Exception as e:
        report.results.append(
            ValidationResult(
                name="file_readable",
                passed=False,
                message=f"Failed to read file: {e}",
            )
        )
        return report

    report.results.append(
        ValidationResult(
            name="file_readable",
            passed=True,
            message="File loaded successfully",
        )
    )

    # Run all validation checks
    report.results.extend(
        [
            validate_schema(
                gdf,
                OBS_V1_REQUIRED_COLUMNS if schema_version == "obs-v1" else REQUIRED_COLUMNS,
            ),
            validate_geometry(gdf),
            validate_timestamps(gdf),
            validate_physical_bounds(gdf),
            validate_locations(gdf, us_only=us_only),
            validate_region_codes(gdf, us_only=us_only),
            validate_no_duplicates(gdf),
            validate_null_rates(gdf),
            validate_record_count(gdf, min_records),
            validate_station_count(gdf, min_stations),
            validate_metric_imperial_consistency(gdf),
        ]
    )

    # Gap validation (if station metadata provided)
    if expected_stations is not None and year is not None:
        report.results.append(validate_station_coverage(gdf, expected_stations, year))
        report.results.append(validate_temporal_completeness(gdf))

    return report
