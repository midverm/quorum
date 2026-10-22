# Quorum benchmarks

```sh
make benchmark
```

By default, we generate **50,000 keys with 512-byte values**, then test:

- **Writes:** Store the data with `sync=false`, then repeat with `sync=true`
  in a fresh database.
- **Cold reads:** Clear cached file data, reopen the database, and read shuffled
  keys from saved disk blocks.
- **Cached reads:** Warm the cache, then read those same keys again.

We repeat each test **3 times** and show the middle result.

We report **operations/sec, MB/sec, average latency, and p99 latency**.
Every read is checked for the correct value.

The default output is an aligned, four-row summary: operation, sync setting,
ops/sec, MB/sec, average latency, and p99 latency. Each value is the median
across trials. Higher throughput is better; lower latency is better. `us` means
microseconds; p99 is the time within which 99% of calls finished in a trial.

Choose dataset size, value size, repeats, and an optional existing data directory,
or save individual trial measurements as CSV:

```sh
make
./build/kv_bench 50000 512 3
./build/kv_bench 50000 512 3 /Volumes/MyDisk
./build/kv_bench --csv 50000 512 3 > bench/results.csv
```

Defaults: 50,000 entries, 16-byte keys, 512-byte values, three trials, one client,
and a fresh temporary database for every write setting in each trial. Inputs use
fixed random seeds and are generated before timing. Sync order alternates.

| Measurement | Method |
| --- | --- |
| Sequential writes | Insert every key with `sync=false`, then test a separate fresh DB with `sync=true`. |
| Cold random reads | Close the DB, settle background compaction, select one key per persisted SST block, shuffle the keys, invalidate those files' OS cache pages, reopen, and read each sampled key once. |
| Cached random reads | Warm the DB cache, then repeat exactly the same sampled keys in the same order. |

Sync applies only to writes. Cold/cached reads run once per trial using the
`sync=false` write fixture after `Close()` has synchronized it. Read rows show
`-` in the summary and an empty sync field in CSV.

Every read is compared byte-for-byte with the generated input. Unflushed keys
recovered into the memtable are excluded from read samples. Cold/cached sample
counts therefore differ from the write count. The persisted data must fit the
existing 32 MiB block cache; use fewer entries or smaller values if it does not.

CSV reports ops/sec, payload MB/sec and MiB/sec, mean/p50/p95/p99/max API latency
in microseconds, and process storage-read bytes. MB/sec uses 1,000,000 bytes;
MiB/sec uses 1,048,576. Both count value bytes only. Per-call latency excludes
validation; throughput includes timing and validation overhead. Setup, open,
close, compaction settling, and cache preparation are outside measurement windows.

Cold reads use macOS `msync(MS_INVALIDATE)` and per-file cache controls on the
temporary SST files. The run requires positive storage I/O for cold reads and
zero for cached reads. OS shared pages and device caches can still affect cold
first-pass results. No global cache purge is used. `Get` has no sync option.

No DB source or private fields are changed. Temporary databases are removed
after each run. Latest measurements: [results.csv](results.csv).
