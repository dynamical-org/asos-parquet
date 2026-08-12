# MSC SWOB realtime observations

The adapter consumes canonical XML files from ECCC's Datamart and uses GeoMet's
`swob-realtime` collection only for discovery. A cumulative JSON manifest drives
deterministic 2026 replay:

```json
[
  {
    "path": "incoming/2026-08-12-1800-CADN-AUTO-swob.xml",
    "uri": "https://dd.weather.gc.ca/.../2026-08-12-1800-CADN-AUTO-swob.xml",
    "network": "MSC",
    "media_type": "application/xml",
    "ingested_at": "2026-08-12T18:03:00+00:00",
    "sha256": "<64 lowercase hex characters>",
    "source_published_at": null
  }
]
```

Run:

```bash
uv run python scripts/rebuild_swob_2026.py --manifest manifest.json --output data/obs-parquet/v1/canonical/msc-swob
```

Station-list CSV revisions belong in the same manifest with `media_type` set to
`text/csv`; they are archived but not treated as observations. Raw objects are stored
by SHA-256 before normalization.

The adapter maps instantaneous temperature, dew point and relative humidity; hourly
precipitation; present weather; and ten-minute wind speed, direction and gust. An
absent or `MSNG` precipitation element emits no observation, so airport precipitation
is never synthesized as zero. Capabilities are calculated independently by variable.
Precipitation type remains an independent absent capability unless a supported SWOB
field is observed; present weather is not silently promoted to precipitation type.

`watermark.json` reports each provider network's latest observation and availability,
measured publication latency, record count, and a `source_gap` alert when the latest
observation is more than two hours behind raw ingest.

## Historical reconciliation

SWOB is the low-latency source and ECCC climate-hourly is the historical source. The
historical adapter must map the same station identity before applying source
precedence. SWOB supplies data after its first archived 2026 record; historical data
fills earlier dates and gaps, but must not turn missing SWOB precipitation into zero.
This PR cannot verify final station reconciliation because issue #29 is not yet
implemented.

## Verification performed 2026-08-12

- The official GeoMet API documented `swob-realtime` as a rolling 30-day collection.
- Current core MSC and ON-MNR-AFFES Datamart files used point-observation XML 2.0 and
  exposed `qa_summary` qualifiers.
- The current partner sample exposed `rnfl_amt_pst1hr`; the official product guide and
  GeoMet schema also expose `pcpn_amt_pst1hr`.
- Core and partner station-list CSVs were reachable from the official Datamart.
- A malformed DFO record dated 2101 appeared first under descending date sort. Replay
  therefore enforces the 2026 target rather than trusting discovery ordering.
- The committed fixtures are minimized from observed schemas; they are not assertions
  that every provider emits every variable.

Official references:

- https://api.weather.gc.ca/collections/swob-realtime
- https://api.weather.gc.ca/collections/swob-realtime/schema
- https://dd4.weather.gc.ca/observations/doc/SWOB-ML_Product_User_Guide_v8.14_e.pdf
- https://wis2-gdc.weather.gc.ca/collections/wis2-discovery-metadata/items/urn%3Awmo%3Amd%3Aca-eccc-msc%3Ae50a9544-eee2-460c-a8b1-1a92a487d060
