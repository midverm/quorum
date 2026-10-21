#pragma once

#include "sstable.h"
#include "status.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace quorum {

struct VersionEdit {
    uint64_t lastSequence = 0;
    uint64_t oldestRequiredWal = 1;
    uint64_t nextFileNumber = 1;
    std::vector<FileMetadata> added;
    std::vector<std::pair<uint32_t, uint64_t>> removed;
};

class Manifest {
public:
    Manifest() = default;
    ~Manifest();
    Manifest(const Manifest&) = delete;
    Manifest& operator=(const Manifest&) = delete;
    Status Create(const std::string& path);
    Status Recover(const std::string& path, std::vector<VersionEdit>& edits);
    Status AppendAndSync(const VersionEdit& edit);
    Status Close();

private:
    static constexpr uint32_t kManifestFormat = 0x514d0001;
    static Status DecodeEdit(std::string_view bytes, VersionEdit& edit);
    int fd_ = -1;
    Status error_;
};

}
