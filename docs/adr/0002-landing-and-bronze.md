# ADR 0002: Landing contract and idempotent Bronze loads

Status: accepted (Stage 1)

## Decisions

**Raw means the bytes the source served.** The collector uploads the file exactly as downloaded
(Binance: the daily zip), after verifying it against the source's published SHA-256. Nothing is
decompressed or rewritten on the VM. Raw files are never edited or deleted.

**A manifest sidecar is the commit marker.** For every landed file the collector writes
`<file>.manifest.json` next to it, and only after the data file's upload has completed and its size
has been checked. The Files API has no rename, so this ordering is what makes a landing atomic from the
platform's point of view: a file without a manifest does not exist as far as any job is concerned.
The manifest carries the source URL, checksum, size, covered period and fetch time, which is the
"what, when and from where" record the specification asks for. `pitlake.manifest.LandingManifest` is
the single definition both sides import.

**The landing volume is the collector's only state.** A partition is done when its manifest exists.
Backfill, daily catch-up and recovery after downtime are therefore the same code path, and the VM can
be rebuilt from scratch without losing anything.

**Manifests reach Databricks through Auto Loader.** The `discover_manifests` task streams new
`*.manifest.json` files into `control.landing_manifest` with an `availableNow` trigger. The checkpoint
lives in the `control.checkpoints` volume. The table is append-only; a re-fetched file appears again
with its new checksum and the latest row per `landing_path` wins.

**Bronze loads are idempotent by construction.** `load_bronze` overwrites exactly the rows whose
`_source_file` is in the batch (Delta `replaceWhere`), in a single transaction. Loading a file twice
yields one copy of its rows. `control.bronze_load_log` only decides what is pending; if a run fails
between the write and the log append, the next run reloads the same files with the same result.

**Bronze is strings.** Every raw column is stored as STRING with a `_corrupt_record` column for lines
that do not parse, so an upstream format change is preserved for Silver to judge rather than failing
the load. Metadata columns start with an underscore.

**The loader is generic.** Everything source-specific the platform needs is a `Dataset` entry in
`pitlake.datasets`; everything source-specific the collector needs is a `Source` subclass. Stage 2
adds entries, not branches.

## Consequences and hazards

- Zip archives are decompressed into the `bronze.staging` volume during a load and removed afterwards.
  Spark cannot read zip directly; this is the price of keeping raw files byte-identical to the source.
- Binance switched spot trade timestamps from milliseconds to microseconds on 2025-01-01. Bronze keeps
  both as received; Silver's plausible-range rule (Stage 3) must normalise them.
- Deleting `control.checkpoints/landing_manifest` makes Auto Loader re-read every manifest. That only
  duplicates rows in the append-only manifest table, which the latest-row rule already tolerates.
- `--reload true` requires an explicit period so a typo cannot reload the whole history.
