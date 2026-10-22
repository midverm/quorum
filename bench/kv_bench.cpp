#include "db.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
#if defined(__APPLE__)
#include <libproc.h>
#include <sys/resource.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

void Check(const quorum::Status& status, const char* operation) {
    if (!status.ok()) throw std::runtime_error(std::string(operation) + ": " + status.message());
}

// Only remove a directory created by this process, never a supplied database path.
class TemporaryDirectory {
public:
    explicit TemporaryDirectory(const fs::path& parent) {
        std::string pattern = (parent / "quorum-bench-XXXXXX").string();
        if (!::mkdtemp(pattern.data())) throw std::runtime_error("cannot create benchmark directory");
        path = std::move(pattern);
    }
    ~TemporaryDirectory() {
        std::error_code error;
        fs::remove_all(path, error);
        if (error) std::cerr << "Cleanup failed for " << path << ": " << error.message() << '\n';
    }
    fs::path path;
};

struct Entry {
    std::string key;
    std::string value;
};

std::vector<Entry> MakeEntries(std::size_t count, std::size_t valueBytes) {
    std::mt19937_64 random(42);
    std::vector<Entry> entries;
    entries.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        std::string key(16, '0');
        auto number = static_cast<uint64_t>(i);
        for (int digit = 15; digit >= 0; --digit, number >>= 4)
            key[digit] = "0123456789abcdef"[number & 15];
        std::string value(valueBytes, '\0');
        for (auto& byte : value) byte = static_cast<char>(random() & 255);
        entries.push_back({std::move(key), std::move(value)});
    }
    return entries;
}

std::size_t ParseCount(const char* text, const char* name, std::size_t maximum) {
    std::size_t value = 0;
    const std::string input(text);
    const auto parsed = std::from_chars(input.data(), input.data() + input.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() ||
        value == 0 || value > maximum)
        throw std::runtime_error(std::string(name) + " must be between 1 and " + std::to_string(maximum));
    return value;
}

// Inspect only closed, private benchmark databases: Recover may repair a manifest tail.
std::vector<quorum::FileMetadata> LiveTables(const fs::path& path) {
    quorum::Manifest manifest;
    std::vector<quorum::VersionEdit> edits;
    Check(manifest.Recover((path / "MANIFEST").string(), edits), "Read benchmark manifest");
    std::map<uint64_t, quorum::FileMetadata> live;
    for (const auto& edit : edits) {
        for (const auto& removed : edit.removed) live.erase(removed.second);
        for (const auto& added : edit.added) live[added.number] = added;
    }
    std::vector<quorum::FileMetadata> tables;
    for (const auto& item : live) tables.push_back(item.second);
    return tables;
}

bool NeedsCompaction(const std::vector<quorum::FileMetadata>& tables) {
    std::size_t levelZero = 0;
    uint64_t bytes[32]{};
    for (const auto& table : tables) {
        levelZero += table.level == 0;
        bytes[table.level] += table.fileSize;
    }
    if (levelZero >= 4) return true;
    long double limit = 10 * 1024 * 1024;
    for (int level = 1; level < 31; ++level, limit *= 10)
        if (bytes[level] >= limit) return true;
    return false;
}

std::vector<std::string> BlockKeys(const fs::path& path,
                                 const std::vector<quorum::FileMetadata>& tables) {
    std::vector<std::string> keys;
    uint64_t blockBytes = 0;
    for (const auto& table : tables) {
        const auto filename = path / quorum::FileName(table.number, ".sst");
        const int fd = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) Check(quorum::FileError("open SST index"), "Block keys");
        quorum::Status status;
        std::string footer, index;
        status = quorum::ReadAt(fd, table.fileSize - 40, 40, footer);
        uint64_t ignored = 0, offset = 0, size = 0;
        std::string_view view(footer);
        if (status.ok() && (!quorum::Take64(view, ignored) || !quorum::Take64(view, ignored) ||
                            !quorum::Take64(view, offset) || !quorum::Take64(view, size)))
            status = quorum::Status::Corruption("invalid benchmark SST footer");
        bool complete = false;
        if (status.ok()) status = quorum::ReadRecord(fd, offset + size, offset, index, complete);
        ::close(fd);
        Check(status, "Read SST index");
        if (!complete) throw std::runtime_error("incomplete benchmark SST index");
        view = index;
        uint32_t count = 0;
        if (!quorum::Take32(view, count)) throw std::runtime_error("invalid block count");
        for (uint32_t i = 0; i < count; ++i) {
            std::string lastKey;
            uint64_t blockOffset = 0, blockSize = 0;
            if (!quorum::TakeString(view, lastKey) || !quorum::ValidInternalKey(lastKey) ||
                !quorum::Take64(view, blockOffset) || !quorum::Take64(view, blockSize))
                throw std::runtime_error("invalid block index entry");
            keys.emplace_back(quorum::UserKey(lastKey));
            blockBytes += blockSize;
        }
        if (!view.empty()) throw std::runtime_error("trailing benchmark index bytes");
    }
    if (keys.empty()) throw std::runtime_error("no persisted blocks; increase the entry count");
    // Every sampled block must remain resident for the fully cached comparison.
    if (blockBytes > 32 * 1024 * 1024)
        throw std::runtime_error("SST data exceeds 32 MiB cache; reduce the entry count or value size");
    std::sort(keys.begin(), keys.end());
    if (std::adjacent_find(keys.begin(), keys.end()) != keys.end())
        throw std::runtime_error("overlapping block samples; benchmark requires unique fresh inserts");
    std::mt19937_64 random(12345);
    std::shuffle(keys.begin(), keys.end(), random);
    return keys;
}

void FileCaching(const fs::path& path, const std::vector<quorum::FileMetadata>& tables, bool enabled) {
#if defined(__APPLE__)
    for (const auto& table : tables) {
        const auto filename = path / quorum::FileName(table.number, ".sst");
        const int fd = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) Check(quorum::FileError("open cache-control file"), "File caching");
        quorum::Status status;
        if (::fcntl(fd, F_GLOBAL_NOCACHE, enabled ? 0 : 1) == -1 ||
            (!enabled && ::fcntl(fd, F_RDAHEAD, 0) == -1))
            status = quorum::FileError("set file cache policy");
        // Discard existing clean pages as well as preventing new pages from being retained.
        if (!enabled && status.ok()) {
            void* mapping = ::mmap(nullptr, table.fileSize, PROT_READ, MAP_SHARED, fd, 0);
            if (mapping == MAP_FAILED) status = quorum::FileError("map cache-control file");
            else {
                if (::msync(mapping, table.fileSize, MS_SYNC | MS_INVALIDATE) != 0)
                    status = quorum::FileError("invalidate benchmark file pages");
                ::munmap(mapping, table.fileSize);
            }
        }
        ::close(fd);
        Check(status, "File caching");
    }
#else
    (void)path; (void)tables; (void)enabled;
    throw std::runtime_error("cold reads require macOS file cache controls");
#endif
}

int64_t StorageReadBytes() {
#if defined(__APPLE__)
    rusage_info_v2 usage{};
    if (::proc_pid_rusage(::getpid(), RUSAGE_INFO_V2, reinterpret_cast<rusage_info_t*>(&usage)) == 0)
        return static_cast<int64_t>(usage.ri_diskio_bytesread);
#endif
    return -1;
}

struct Result {
    std::size_t trial;
    std::optional<bool> sync;
    const char* operation;
    std::size_t count;
    double seconds, rate, mean, p50, p95, p99, maximum;
    int64_t diskBytes;
};

template <typename Operation, typename Verify>
Result Measure(std::size_t trial, std::optional<bool> sync, const char* name, std::size_t count,
                Operation operation, Verify verify) {
    std::vector<double> latency(count);
    const auto diskBefore = StorageReadBytes();
    const auto start = Clock::now();
    for (std::size_t i = 0; i < count; ++i) {
        const auto callStart = Clock::now();
        const auto status = operation(i);
        latency[i] = std::chrono::duration<double, std::micro>(Clock::now() - callStart).count();
        Check(status, name);
        verify(i);
    }
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
    const auto diskAfter = StorageReadBytes();
    const auto diskBytes = diskBefore >= 0 && diskAfter >= diskBefore ? diskAfter - diskBefore : -1;
    const double rate = count / seconds;
    double mean = 0;
    for (const double value : latency) mean += value / count;
    std::sort(latency.begin(), latency.end());
    auto percentile = [&](double fraction) {
        return latency[static_cast<std::size_t>(std::ceil(fraction * count)) - 1];
    };
    return {trial, sync, name, count, seconds, rate, mean, percentile(.50), percentile(.95),
            percentile(.99), latency.back(), diskBytes};
}

void PrintResults(const std::vector<Result>& results, std::size_t valueBytes, bool csv) {
    if (csv) {
        std::cout << "trial,sync,operation,operations,seconds,ops_per_sec,mb_per_sec,mib_per_sec,"
                     "mean_us,p50_us,p95_us,p99_us,max_us,storage_read_bytes\n";
        for (const auto& result : results) {
            std::cout << result.trial << ',';
            if (result.sync.has_value()) std::cout << (*result.sync ? "true" : "false");
            std::cout << ',' << result.operation << ',' << result.count << ',' << std::fixed
                      << std::setprecision(6) << result.seconds << ',' << std::setprecision(2)
                      << result.rate << ',' << result.rate * valueBytes / 1e6
                      << ',' << result.rate * valueBytes / (1024 * 1024) << ',' << result.mean
                      << ',' << result.p50 << ',' << result.p95 << ',' << result.p99
                      << ',' << result.maximum << ',';
            if (result.diskBytes >= 0) std::cout << result.diskBytes;
            std::cout << '\n';
        }
        return;
    }
    std::cout << "\nResults (median across trials)\n\n"
              << std::left << std::setw(23) << "Operation" << std::setw(8) << "Sync"
              << std::right << std::setw(12) << "Ops/sec" << std::setw(11) << "MB/sec"
              << std::setw(12) << "Avg (us)" << std::setw(12) << "p99 (us)"
              << '\n' << std::string(78, '-') << '\n';
    const char* operations[] = {"write_sequential", "write_sequential", "read_cold", "read_cached"};
    const char* labels[] = {"Sequential writes", "Sequential writes", "Cold random reads", "Cached random reads"};
    const std::optional<bool> settings[] = {false, true, std::nullopt, std::nullopt};
    for (std::size_t i = 0; i < 4; ++i) {
        const auto sync = settings[i];
        auto median = [&](double Result::* field) {
            std::vector<double> values;
            for (const auto& result : results)
                if (result.sync == sync && std::string(result.operation) == operations[i])
                    values.push_back(result.*field);
            std::sort(values.begin(), values.end());
            const auto middle = values.size() / 2;
            return values.size() % 2 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
        };
        const auto rate = median(&Result::rate);
        const char* syncLabel = sync.has_value() ? (*sync ? "true" : "false") : "-";
        std::cout << std::left << std::setw(23) << labels[i] << std::setw(8) << syncLabel
                  << std::right << std::fixed << std::setprecision(0) << std::setw(12) << rate
                  << std::setprecision(2) << std::setw(11) << rate * valueBytes / 1e6
                  << std::setw(12) << median(&Result::mean) << std::setw(12) << median(&Result::p99) << '\n';
    }
    std::cout << "\nOps/sec and MB/sec: higher is faster. Latency: lower is faster.\n"
              << "us = microseconds. p99 = 99% of calls finished within this time.\n"
              << "MB/sec counts value bytes. Sync applies to writes only; '-' means not applicable.\n"
              << "Verified: read values match; cold reads used storage; cached reads used no storage I/O.\n";
}

void Run(std::size_t trial, bool sync, const fs::path& parent, const std::vector<Entry>& entries,
         std::vector<Result>& results) {
    TemporaryDirectory temporary(parent);
    const auto path = temporary.path / "db";
    std::cerr << "Trial " << trial << ", sync=" << std::boolalpha << sync << '\n';
    {
        quorum::DB writer(path.string());
        Check(writer.Open(), "Open writer");
        results.push_back(Measure(trial, sync, "write_sequential", entries.size(),
                [&](std::size_t i) { return writer.Put(entries[i].key, entries[i].value, sync); },
                [](std::size_t) {}));
        Check(writer.Close(), "Close writer");
    }
    // Read behavior is independent of per-write sync. Use one closed fixture per trial.
    if (sync) return;

    // Settle background compaction through the public API before inspecting closed files.
    auto tables = LiveTables(path);
    for (int attempt = 0; NeedsCompaction(tables); ++attempt) {
        if (attempt == 50) throw std::runtime_error("compaction did not settle");
        quorum::DB settling(path.string());
        Check(settling.Open(), "Open to settle compaction");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        Check(settling.Close(), "Close after compaction");
        tables = LiveTables(path);
    }
    const auto keys = BlockKeys(path, tables);
    std::vector<std::size_t> samples;
    samples.reserve(keys.size());
    for (const auto& key : keys) {
        std::size_t index = 0;
        const auto parsed = std::from_chars(key.data(), key.data() + key.size(), index, 16);
        if (parsed.ec != std::errc{} || parsed.ptr != key.data() + key.size() ||
            index >= entries.size() || entries[index].key != key)
            throw std::runtime_error("persisted key does not match benchmark input");
        samples.push_back(index);
    }
    std::cerr << "  Read samples: " << samples.size() << " keys (cold and cached)\n";
    FileCaching(path, tables, false);
    quorum::DB reader(path.string());
    Check(reader.Open(), "Open cold reader");
    std::string actual;
    auto get = [&](std::size_t i) { return reader.Get(entries[samples[i]].key, actual); };
    auto verify = [&](std::size_t i) {
        if (actual != entries[samples[i]].value) throw std::runtime_error("read value mismatch");
    };
    const auto cold = Measure(trial, std::nullopt, "read_cold", samples.size(), get, verify);
    if (cold.diskBytes <= 0) throw std::runtime_error("cold read requires verified storage I/O");
    results.push_back(cold);

    FileCaching(path, tables, true);
    for (std::size_t i = 0; i < samples.size(); ++i) {
        Check(get(i), "Warm cache");
        verify(i);
    }
    const auto cached = Measure(trial, std::nullopt, "read_cached", samples.size(), get, verify);
    if (cached.diskBytes != 0) throw std::runtime_error("cached read requires zero storage I/O");
    results.push_back(cached);
    Check(reader.Close(), "Close reader");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--help") {
        std::cout << "Usage: " << argv[0] << " [--csv] [entries=50000] [value_bytes=512] [repeats=3] [temp_parent]\n"
                  << "Sequential writes with sync=false/true; one cold and one cached read test per trial.\n"
                  << "Default: readable summary. --csv: individual trial measurements on stdout.\n"
                  << "Progress goes to stderr. Cold reads require macOS.\n";
        return 0;
    }
    try {
#if !defined(__APPLE__)
        throw std::runtime_error("cold reads require macOS file cache controls");
#endif
        const bool csv = argc > 1 && std::string(argv[1]) == "--csv";
        const int offset = csv ? 1 : 0;
        if (argc > 5 + offset) throw std::runtime_error("too many arguments; use --help");
        const auto count = argc > 1 + offset ? ParseCount(argv[1 + offset], "entries", 100000000) : 50000;
        const auto valueBytes = argc > 2 + offset
            ? ParseCount(argv[2 + offset], "value_bytes", 64 * 1024 * 1024 - 64) : 512;
        const auto repeats = argc > 3 + offset ? ParseCount(argv[3 + offset], "repeats", 1000) : 3;
        const auto parent = argc > 4 + offset ? fs::path(argv[4 + offset]) : fs::temp_directory_path();
        if (!fs::is_directory(parent)) throw std::runtime_error("temp_parent must be an existing directory");
        const auto entries = MakeEntries(count, valueBytes);
        auto& info = csv ? std::cerr : std::cout;
        info << "Quorum benchmark\n"
             << "  Entries: " << count << " | Key: 16 bytes | Value: " << valueBytes << " bytes\n"
             << "  Trials: " << repeats << " | Write sync: false and true | One client\n\n" << std::flush;
        std::vector<Result> results;
        results.reserve(repeats * 4);
        for (std::size_t trial = 1; trial <= repeats; ++trial) {
            const bool firstSync = trial % 2 == 0;
            for (const bool sync : {firstSync, !firstSync}) Run(trial, sync, parent, entries, results);
        }
        PrintResults(results, valueBytes, csv);
    } catch (const std::exception& error) {
        std::cerr << "Benchmark failed: " << error.what() << '\n';
        return 1;
    }
}
