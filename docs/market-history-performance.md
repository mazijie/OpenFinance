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

## Full snapshot layout experiment

`scripts/benchmark_market_layout.py` runs the history API in a separate backend
process against one pinned table snapshot. It records endpoint, file planning,
and Arrow read durations, candidate file sizes, bar hashes, and any separate
availability check as JSON Lines. This is an in-process HTTP client, not a
browser or network benchmark. Cache state is uncontrolled; candidate file sizes
are not actual bytes transferred. By default availability is checked once per
symbol and period when the production index does not match the candidate.
`scripts/report_market_layout.py` summarizes samples and checks result hashes;
use at least ten iterations per symbol and period.

Example after creating and validating candidates (replace identifiers and paths):

```sh
docker exec openfinance-backend python -m scripts.benchmark_market_layout \
  --table bench_example.sorted --snapshot 12345 --iterations 10 \
  --output /app/output/example/sorted.jsonl
python scripts/report_market_layout.py output/example/*.jsonl
```

`scripts/run_market_layout_read_matrix.py` accepts a prepared experiment result
directory and runs three symbol streams concurrently. Those independent client
processes have separate in-process query gates; the resulting timings are not a
measurement of production's single-worker queue. Its append and compaction
counterparts can wait for the previous stage's validated report.

Use a new `bench_<dataset>` namespace and retain the source snapshot throughout
the experiment. `backend.maintenance.clone_market_manifest_benchmark` registers
a metadata-isolated clone with its own UUID and metadata location. The clone's
old manifests and all its data files **are shared with the source snapshot**;
only `rewrite-manifests` is permitted. Never purge it, expire its snapshots, or
run data-file rewrites on it. The clone disables Iceberg garbage collection.
`scripts/compare_market_manifest_files.py` verifies the exact source and target
immutable file references and rejects delete files. It does not protect against
an external process deleting or modifying those shared objects.

`spark/benchmark/market_layout_experiment.py` creates independent tables from the
pinned source snapshot with monthly, 16-bucket, or 32-bucket layouts; its sort
action rewrites only the independent table named `sorted`. Compare each candidate
against the original snapshot with `exceptAll` in both directions. Spark CTAS may
relax field nullability; record this difference and preserve the original schema
in any production implementation.

After recording read benchmarks, `scripts/simulate_market_layout_append.py`
copies a real source day's rows to a clearly synthetic future date and appends
them to each independent candidate using the production PyIceberg append function.
The default batch of 200 models ten normalization inputs of 20 stocks each; it
does not measure upsert/version checks or locking. The candidate snapshot changes,
so record its pre-append snapshot and run this only after comparisons and reads.
The script refuses the shared-file manifest clone and the production namespace.

The experiment's `compact-day` action applies bin-packing to the entire month
containing the synthetic day, with a minimum input-file threshold. It pins the
pre-compaction candidate snapshot and compares the month's rows before and after
the rewrite. `scripts/run_market_layout_compaction_matrix.py` runs this only after
the append simulation. It is a benchmark; choosing a threshold and rollout
requires the measured file growth and latency.

The separate `openfinance_market_history_compaction` Airflow DAG is created
paused. When enabled, it rewrites eligible files for the current and preceding
calendar months weekly, sorting by `symbol, trading_date`, then requests a
refresh of the snapshot-specific history index. The index builder skips its run
if another rebuild holds its lock; concurrent later commits can also make the
index stale again. Its initial file-count trigger is eight input files and
target size is 128 MiB; Iceberg can also select files for size or delete-file
criteria. These are starting settings, not validated optima. It
uses Iceberg's atomic rewrite and conflict validation; it does not hold the
project-wide writer lock while Spark runs. A concurrent conflicting update can
therefore cause the task to fail and retry. A successful rewrite changes the
snapshot, and history queries fall back until the index is refreshed. Historical
months outside the rolling window need a separately bounded invocation of
`spark/jobs/compact_market_history.py`.

Daily ingestion still fetches small API batches to Bronze, then groups up to
`A_SHARE_NORMALIZATION_BATCH_SIZE` objects (default 300) for each Silver
normalization job. Backfill waits for the collected Bronze paths and groups up
to `A_SHARE_BACKFILL_NORMALIZATION_BATCH_SIZE` objects (default 5), including
successful partial-recovery objects. Both limits bound the number of objects,
not their bytes or rows; normalization holds each batch's records in memory.
The daily writer may commit new and changed records separately, so one job is
not necessarily one Iceberg snapshot.

The benchmark Spark actions reject destinations outside an isolated `bench_` namespace.
Run preparation and timed reads separately to avoid concurrent Spark work
distorting measurements; preserve per-action JSON and logs in `output/` or
`.cache/`. The Spark submitter must explicitly allocate executor cores and
memory; changing `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` only changes the
worker's available capacity. Do not treat a successful benchmark as permission
to rewrite or replace the production table.

## November 10, 2026 isolated comparison

The source snapshot contained 17,054,786 rows, 97,606 data files and 558
manifests. The manifest-only clone kept exactly the same data files and reduced
the manifests to nine. Independent candidate tables compared equal to the
source for all rows, but their Spark-created Iceberg schemas changed required
fields to optional, so they are not suitable for production replacement.

The test ran the actual history API in three concurrent independent processes
for three symbols, three periods and ten requests per group. The table below
shows the median across the 30 observations for each period, not single-client
latency. Candidate availability lookups used a fallback because the index did
not match their snapshots. File-size figures are sums of selected file sizes,
not measured network bytes. Shared server load and cache also affect results.

| Layout | Daily median | Weekly median | Monthly median | Daily selected file size |
| --- | ---: | ---: | ---: | ---: |
| Source | 14.90 s | 15.74 s | 15.60 s | 0.29 MiB |
| Manifests only | 0.89 s | 2.24 s | 4.26 s | 0.29 MiB |
| Month, sorted | 0.18 s | 0.36 s | 0.53 s | 37.72 MiB |
| 16 symbol buckets | 1.22 s | 1.48 s | 1.58 s | 2.65 MiB |
| 32 symbol buckets | 2.31 s | 2.37 s | 2.56 s | 1.48 MiB |

The 16- and 32-bucket candidates initially had 14,475 and 28,367 files,
respectively. A synthetic day with 5,570 records split into 28 appends of up to
200 records added 30 files to the sorted monthly table, 475 to 16 buckets and
936 to 32 buckets. This measures the append function, not the full ingestion
pipeline, version lookups or retries. Compaction with a minimum of eight input
files rewrote 28 to 2, 443 to 32, and 872 to 64 files, respectively; the
synthetic month's 5,570 rows were identical before and after in every case.
Some files stayed below the rewrite threshold. A separate single-batch append
smoke test generated three files for monthly partitions and 48 for 16 buckets.
The daily pipeline now attempts to group up to 300 Bronze objects, so the
200-record simulation models the earlier smaller-batch setting, not its current
usual commit size. Full results and logs are in `output/market_layout_20261110`.

On November 10 the production table's manifests were rewritten in place, from
558 to nine, holding the shared Iceberg writer lock. The table UUID and complete
Iceberg schema were unchanged. A before/after snapshot comparison verified the
same 97,606 immutable data-file paths, sizes and record counts and no delete
files. The history-index state was retargeted only after this equality check,
retaining its validity window. No production data files were compacted by this
operation. Three sequential requests for `000001.SZ` against the live backend
afterward took approximately 1.05–1.27 seconds daily, 2.39–2.75 seconds weekly
and 4.23–4.92 seconds monthly; this is a small live sample, not the same load as
the concurrent benchmark above.
