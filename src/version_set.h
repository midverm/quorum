#pragma once

#include "manifest.h"
#include "memtable.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace quorum {

struct TableFile {
    FileMetadata metadata;
    std::string path;
    std::atomic<bool> obsolete{false};
    ~TableFile();
};

struct Version {
    std::vector<std::shared_ptr<TableFile>> files;
};

class VersionSet {
public:
    explicit VersionSet(std::string path) : path_(std::move(path)), current_(std::make_shared<Version>()) {}
    std::shared_ptr<const Version> Current() const { return current_; }
    uint64_t NewFileNumber();
    uint64_t NextFileNumber() const { return nextFileNumber_; }
    void SetNextFileNumber(uint64_t number) { nextFileNumber_ = number; }
    Status BuildNext(const VersionEdit& edit, std::shared_ptr<Version>& next) const;
    void Publish(std::shared_ptr<Version> next);
    GetResult Get(std::string_view key, uint64_t snapshotSeq,
                  const std::shared_ptr<const Version>& version) const;
    int CompactionLevel() const;
    bool NeedsCompaction() const { return CompactionLevel() >= 0; }
    void Clear() { current_.reset(); }

private:
    std::string path_;
    std::shared_ptr<Version> current_;
    uint64_t nextFileNumber_ = 1;
};

}
