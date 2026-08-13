# Source and dataset composition

Source ingestion and dataset publication are independent operations.

- A source adapter produces a named `SourceFrame` without choosing a published dataset.
- A composer selects source frames and enforces one dataset schema.
- A publisher writes a composed frame to an explicit destination.

`AsosParquetComposer` accepts only the IEM source and enforces `asos_parquet_schema`. The scheduled updater continues to publish this composition to `asos-parquet`.

`obs_schema` is a separate contract. ECCC, SWOB, DWD, and later adapters can feed an obs composer without changing the ASOS composer or its destination. Adding those sources does not imply that every variable is present at every station; capabilities remain source-, station-, and variable-specific.

The intended execution shape is:

```text
IEM capture  ──┬──> AsosParquetComposer ──> asos-parquet
               └──> ObsComposer ──────────> obs-parquet
ECCC capture ─────> ObsComposer
SWOB capture ─────> ObsComposer
DWD capture  ─────> ObsComposer
```

Schedules invoke a named source capture or dataset composition. Environment variables configure credentials and the destination belonging to that named publisher; they do not select which dataset a generic updater means.

