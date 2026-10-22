# quorum

embedded key-value database in c++17, built around an lsm tree.
log-backed writes, leveled compaction, bloom filters and cached reads.
runs inside your app. data lives in a folder, no separate server.

benchmarked at **115k writes/sec** (sync=false) and **550k cached reads/sec** with 512-byte values on apple m1.

## features

- **memtable:** sorted data in memory. flushed to disk around 4 mib.
- **sstables:** sorted files on disk, arranged in levels.
- **write path:** append to the write-ahead log, then update the memtable. deletes store a marker.
- **read path:** check memory first, then sstables. bloom filters skip tables that can't contain the key.
- **block cache:** keeps recently read disk blocks in memory.
- **compactor:** merges sstables in the background. removes old versions and delete markers when safe.
- **recovery:** the manifest tracks live sstables. replays the log on open to restore unflushed writes.
- **sync writes:** `sync=true` waits for the log to sync. `sync=false` skips that wait, so a crash can lose recent writes.

## try it

needs a c++17 compiler and make.

```sh
make build/quorum
./build/quorum ./mydb put name "hello world"
./build/quorum ./mydb get name
./build/quorum ./mydb delete name
```

`./mydb` is the folder for your data. writes use `sync=true`.
quote keys or values with spaces. missing keys and errors exit with code 1.

## use in c++

```sh
make build/basic
./build/basic ./mydb
```

writes a value, reopens the database, reads it back.
`./mydb` is the folder for your data.

see [example code](examples/basic.cpp) to use it in your own app.

## benchmarks

measured on apple m1, 8 gb ram, macos 27.0.1. one client, 16-byte keys,
512-byte values. 50,000 writes per mode, 6,148 sampled reads per read phase.
each score is the median of 3 runs.

| operation | ops/sec | mb/sec | avg latency (us) | p99 latency (us) |
| --- | ---: | ---: | ---: | ---: |
| sequential writes, sync=false | 115,385 | 59.08 | 8.64 | 15.42 |
| sequential writes, sync=true | 29,182 | 14.94 | 34.24 | 81.96 |
| cold random reads | 22,734 | 11.64 | 43.74 | 281.75 |
| cached random reads | 550,011 | 281.61 | 1.63 | 2.71 |

mb/sec counts value bytes. us means microseconds. p99 is the latency that
99% of calls finished within in a run.

cold reads use a fresh db cache and file-cache invalidation before the first pass.
storage reads are checked: cold runs read from storage, cached runs read zero bytes.
device and os caches can still affect cold results. every returned value is checked.

run it yourself:

```sh
make benchmark
```

tests writes with sync on and off, cold reads and cached reads.
shows ops/sec, mb/sec and latency.

see [benchmark notes](bench/) and [raw scores](bench/results.csv).
