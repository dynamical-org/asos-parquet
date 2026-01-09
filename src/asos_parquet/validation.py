"""Data validation for ASOS geoparquet.

Provides validation checks to ensure data correctness after ingest.
"""

from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas as pd

from .config import DATA_FIELDS, US_STATES


@dataclass
class ValidationResult:
    """Result of a validation check."""

    name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.message}"


@dataclass
class ValidationReport:
    """Complete validation report for a geoparquet file."""

    path: str
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

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
] + DATA_FIELDS

# Physical bounds for weather variables
PHYSICAL_BOUNDS = {
    "tmpf": (-130.0, 150.0),      # Temperature (F): -130 to 150
    "tmpc": (-90.0, 65.0),        # Temperature (C): -90 to 65
    "dwpf": (-130.0, 100.0),      # Dew point (F)
    "dwpc": (-90.0, 40.0),        # Dew point (C)
    "relh": (0.0, 100.0),         # Relative humidity (%)
    "drct": (0.0, 360.0),         # Wind direction (degrees)
    "sknt": (0.0, 250.0),         # Wind speed (knots): max ~250 for extreme gusts
    "gust": (0.0, 300.0),         # Wind gust (knots)
    "alti": (25.0, 32.0),         # Altimeter (inches): ~25-32 for sea level
    "mslp": (850.0, 1100.0),      # Sea level pressure (mb)
    "vsby": (0.0, 100.0),         # Visibility (miles)
    "p01i": (0.0, 20.0),          # 1-hour precip (inches): max ~20 for extreme events
    "p01m": (0.0, 500.0),         # 1-hour precip (mm)
    "longitude": (-180.0, 180.0),
    "latitude": (-90.0, 90.0),
}

# US bounding box (approximate, includes territories)
US_BBOX = {
    "min_lon": -180.0,  # Alaska extends past -180
    "max_lon": -65.0,   # East coast
    "min_lat": 17.0,    # Puerto Rico
    "max_lat": 72.0,    # Alaska
}


def validate_schema(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that all required columns exist."""
    missing = [col for col in REQUIRED_COLUMNS if col not in gdf.columns]

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
        message=f"All {len(REQUIRED_COLUMNS)} required columns present",
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
            message=f"{null_geom:,} null geometries ({null_geom/len(gdf)*100:.1f}%)",
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


def validate_physical_bounds(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that weather values are within physical bounds."""
    out_of_bounds = {}

    for col, (min_val, max_val) in PHYSICAL_BOUNDS.items():
        if col not in gdf.columns:
            continue

        values = gdf[col].dropna()
        if len(values) == 0:
            continue

        below = (values < min_val).sum()
        above = (values > max_val).sum()

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
        return ValidationResult(
            name="physical_bounds",
            passed=False,
            message=f"{total_issues:,} values outside physical bounds in {len(out_of_bounds)} columns",
            details={"columns": out_of_bounds},
        )

    return ValidationResult(
        name="physical_bounds",
        passed=True,
        message="All values within physical bounds",
    )


def validate_us_locations(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that locations are within US boundaries."""
    if "longitude" not in gdf.columns or "latitude" not in gdf.columns:
        return ValidationResult(
            name="us_locations",
            passed=False,
            message="Missing longitude/latitude columns",
        )

    lon = gdf["longitude"].dropna()
    lat = gdf["latitude"].dropna()

    outside_lon = ((lon < US_BBOX["min_lon"]) | (lon > US_BBOX["max_lon"])).sum()
    outside_lat = ((lat < US_BBOX["min_lat"]) | (lat > US_BBOX["max_lat"])).sum()

    if outside_lon > 0 or outside_lat > 0:
        return ValidationResult(
            name="us_locations",
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
        name="us_locations",
        passed=True,
        message=f"All {len(lon):,} locations within US boundaries",
        details={
            "lon_range": (float(lon.min()), float(lon.max())),
            "lat_range": (float(lat.min()), float(lat.max())),
        },
    )


def validate_states(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that state codes are valid US states."""
    if "state" not in gdf.columns:
        return ValidationResult(
            name="states",
            passed=False,
            message="No 'state' column found",
        )

    states = gdf["state"].dropna().unique()
    invalid = [s for s in states if s not in US_STATES]

    if invalid:
        return ValidationResult(
            name="states",
            passed=False,
            message=f"{len(invalid)} invalid state codes: {invalid}",
            details={"invalid": invalid, "valid_count": len(states) - len(invalid)},
        )

    return ValidationResult(
        name="states",
        passed=True,
        message=f"All {len(states)} state codes valid",
        details={"states": sorted(states.tolist())},
    )


def validate_no_duplicates(gdf: gpd.GeoDataFrame) -> ValidationResult:
    """Validate that there are no duplicate (station, valid) pairs."""
    if "station" not in gdf.columns or "valid" not in gdf.columns:
        return ValidationResult(
            name="duplicates",
            passed=False,
            message="Missing station or valid column",
        )

    duplicates = gdf.duplicated(subset=["station", "valid"]).sum()

    if duplicates > 0:
        return ValidationResult(
            name="duplicates",
            passed=False,
            message=f"{duplicates:,} duplicate (station, valid) pairs",
            details={"duplicate_count": int(duplicates)},
        )

    return ValidationResult(
        name="duplicates",
        passed=True,
        message="No duplicate (station, valid) pairs",
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
            message=f"{len(high_null)} columns exceed {max_null_rate*100}% null rate",
            details={"columns": high_null},
        )

    return ValidationResult(
        name="null_rates",
        passed=True,
        message=f"All key columns below {max_null_rate*100}% null rate",
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
    """Validate minimum station count."""
    if "station" not in gdf.columns:
        return ValidationResult(
            name="station_count",
            passed=False,
            message="No 'station' column found",
        )

    count = gdf["station"].nunique()

    if count < min_stations:
        return ValidationResult(
            name="station_count",
            passed=False,
            message=f"Only {count} stations, expected at least {min_stations}",
            details={"count": count, "min_required": min_stations},
        )

    return ValidationResult(
        name="station_count",
        passed=True,
        message=f"{count} unique stations",
        details={"count": count},
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
) -> ValidationReport:
    """Run all validation checks on a geoparquet file.

    Args:
        path: Path to the geoparquet file
        min_records: Minimum expected record count
        min_stations: Minimum expected station count

    Returns:
        ValidationReport with all check results
    """
    path = Path(path)
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
    report.results.extend([
        validate_schema(gdf),
        validate_geometry(gdf),
        validate_timestamps(gdf),
        validate_physical_bounds(gdf),
        validate_us_locations(gdf),
        validate_states(gdf),
        validate_no_duplicates(gdf),
        validate_null_rates(gdf),
        validate_record_count(gdf, min_records),
        validate_station_count(gdf, min_stations),
        validate_metric_imperial_consistency(gdf),
    ])

    return report
