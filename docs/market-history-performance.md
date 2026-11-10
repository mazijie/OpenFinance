# Market history performance

The history page reads an immutable Iceberg snapshot. PostgreSQL stores the earliest
visible date per `(symbol, adjustment_type)` for one complete snapshot. The state
row records the table UUID, snapshot ID, build time, and the next time any currently
invisible record becomes visible. The API trusts the index only when both identifiers
and the visibility window match. A missing symbol in a complete index means no
visible record; a missing or stale generation means unknown. PostgreSQL failure
never makes the API claim that history is absent.

The backend migration `0013_market_history_index` creates the index tables. After
deploying the migration, unpause `openfinance_market_history_index` in Airflow;
it is paused on creation and checks every 15 minutes. It skips an already-current
generation; otherwise Spark scans
the captured snapshot, computes minima and the next visibility transition, and
publishes all rows and the generation in one PostgreSQL transaction. Concurrent
builds are coalesced with an advisory lock. If a snapshot changes while building,
the next run retries. An expired generation is also rebuilt on the next run. A
generation built against a snapshot superseded just after the check is harmless:
the API compares its exact table UUID and snapshot ID before using it.

The `GET /api/market/history` response has `has_more: true`, `false`, or `null`.
Null means the page is ready but availability is unknown. `snapshot_id` is a
string to preserve 64-bit values in JavaScript; `table_uuid`, `query_time`, and
`next_before` identify the exact read. Send those values, `symbol`, and
`adjustment_type` to `GET /api/market/history/availability`. That endpoint uses
the index when valid, otherwise performs the original Iceberg existence scan on
the pinned snapshot. A 410 response means the snapshot is unavailable and the
page must be reloaded. Clients should display the initial page without waiting
for this check and should let users retry a failed check.

Each backend worker admits one history scan at a time and prioritizes page reads
over fallback availability scans. Queued page scans fail with HTTP 503 after 30
seconds and availability scans after 45 seconds rather than wait indefinitely.
This gate is per
process, not a cluster-wide concurrency limit; increasing Uvicorn workers or
backend replicas increases the number of simultaneous Iceberg scans.

## Measurement

After deploying the migration and backend, run the benchmark both serially and
with three periods concurrently, using a representative set of stocks:

```sh
python scripts/benchmark_market_history.py --symbol 600519.SH --iterations 10
python scripts/benchmark_market_history.py --symbol 600519.SH --iterations 10 --concurrent
```

The output separates page and availability timings and errors. The backend logs
`market_history_page_loaded` with query and total seconds and index hit status,
and `market_history_availability_checked` with its source and elapsed seconds.
The Spark job logs the published snapshot and key count, plus current snapshot
file/manifest counts, mean file size, and added/removed files reported by the
current commit. These are snapshot metrics, not a daily sum. Examine files and
manifests using Iceberg metadata tables and monitor these measurements after each
daily ingest and backfill. Compare against the 60-second UI timeout and the
10-second P95 target; neither target is assumed to be met without measurement.

## Isolated layout evaluation

For a bounded, physical copy of one partition, run from the backend container:

```sh
python -m backend.maintenance.copy_market_benchmark_partition --dataset trial1 --month 2026-09 --exchange SZ
```

The command deep-copies the source Parquet files to an isolated benchmark table,
refuses snapshots containing delete files, and records both snapshot IDs. It
recreates manifest metadata, so its manifest layout is not identical to the
original; measure original manifest planning separately. Alternatively,
`spark/benchmark/job.py prepare --kind real` creates a full Spark-rewritten copy,
but its initial data-file layout also differs from the original.

Run `spark/benchmark/market_history_layout.py` against
`openfinance.bench_<dataset>.daily_bars_sample`: `inspect` records file and
manifest counts and Spark scan durations; `rewrite-manifests` and `rewrite-data`
are restricted to benchmark namespaces; `compare --reference
openfinance.openfinance.daily_bars --reference-snapshot <id> --reference-start
<first-day> --reference-end <last-day> --reference-exchange <exchange>
--reference-provider <provider>` checks the copied partition against the pinned
original snapshot. Record results after each operation and
use the API benchmark to evaluate the production reader separately. Spark scan
times do not substitute for PyIceberg planning measurements.

The copied table has independent data files. Keep the original snapshot available
throughout comparison, and coordinate any production maintenance with ingestion.
Do not use the older Silver rebuild script for this experiment: it deduplicates
and replaces tables. Promote no layout change unless row equivalence, snapshot
retention, rollback, and the single/concurrent query measurements have been
verified. Preserve daily updates; batches combine multiple stocks from the same
day and do not require holding daily bars for multiple days.

## Sample comparison, 2026-11-06

A deep copy of the SZ/Tencent September 2026 partition contained 60,805 rows in
291 files (7,236,083 bytes). Bin-pack rewrote those files into one 3,698,270-byte
file; an exact `exceptAll` comparison with the pinned source found zero missing or
extra rows. For `000001.SZ`, PyIceberg selected one file both before and after;
the candidate file grew from 24,587 to 3,698,270 bytes. Five warm single-column
trials gave median planning times of 20.9 ms before and 12.9 ms after, and median
scan times including planning of 26.1 ms and 22.9 ms respectively. Candidate
file size is not a measurement of bytes actually transferred. This small,
single-partition test does not establish the effect on full-table manifests or
concurrent API latency; evaluate those before production maintenance.
