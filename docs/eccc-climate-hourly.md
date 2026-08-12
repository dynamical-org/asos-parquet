# ECCC climate-hourly backfill

The historical Canadian adapter consumes GeoJSON pages from ECCC's sanctioned
`climate-hourly` GeoMet collection. Capture pages with UTC calendar filters such as:

```text
https://api.weather.gc.ca/collections/climate-hourly/items?f=json&limit=10000&UTC_YEAR=2026&UTC_MONTH=8&UTC_DAY=10
```

Follow every response's `next` link verbatim. Store each response immutably and list all pages
in a cumulative manifest:

```json
{
  "source_complete_through": "2026-08-10T23:00:00+00:00",
  "unavailable_intervals": [],
  "payloads": [
    {
      "path": "raw/page-00000.json",
      "uri": "https://api.weather.gc.ca/collections/climate-hourly/items?...",
      "sha256": "<64 lowercase hexadecimal characters>",
      "ingested_at": "2026-08-12T12:00:00+00:00"
    }
  ]
}
```

Build fresh canonical artifacts next to prior datasets:

```bash
make rebuild-eccc ECCC_MANIFEST=/path/to/manifest.json ECCC_OUTPUT=/path/to/eccc-climate-hourly
```

The rebuild archives content-addressed raw pages, emits normalized observations and
per-variable capabilities, and writes explicit watermark and unavailable-interval metadata.
It rejects partial cumulative manifests and completeness regression. Missing precipitation is
stored as unavailable, never as zero.

The reconciliation boundary accepts canonical records from `eccc-climate-hourly` and the
future `eccc-swob` adapter. It retains both candidates, prefers observed over unavailable or
rejected values, then prefers the quality-controlled historical value on otherwise equal
overlaps. It only depends on canonical contracts and does not depend on issue #28 code.

## Verified source behavior

Verified live against `api.weather.gc.ca` on 2026-08-12:

- The collection exposes temperature, dew point, relative humidity, hourly precipitation,
  present weather, wind direction, wind speed, per-variable flags, climate identifiers, and
  `UTC_DATE`.
- `UTC_YEAR`, `UTC_MONTH`, and `UTC_DAY` filters constrain records by UTC calendar day and are
  retained in `next` pagination links.
- 2026-08-10 matched 21,111 records.
- `sortby=-UTC_DATE` returned a latest record at 2026-08-12T07:00:00Z during verification.

The generic OGC `datetime` parameter did not behave as a UTC filter in the live sample, so the
backfill does not rely on it. ECCC flag meanings and a complete 2026 transfer were not verified
in this implementation: flags are preserved without interpretation, and the full replay was
not run because it would transfer and normalize millions of records.
