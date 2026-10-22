#pragma once

#include "block_cache.h"
#include "status.h"

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace quorum {

struct TableFile;
class SSTableReader;

class TableCache {
public:
    explicit TableCache(std::shared_ptr<BlockCache> blockCache = std::make_shared<BlockCache>())
        : blockCache_(std::move(blockCache)) {}
    Status Get(const std::shared_ptr<TableFile>& file, std::shared_ptr<SSTableReader>& reader);
    void Evict(uint64_t fileNumber);

private:
    static constexpr std::size_t kCapacity = 64;
    struct CacheEntry {
        std::shared_ptr<SSTableReader> reader;
        std::list<uint64_t>::iterator lru;
    };
    std::shared_ptr<BlockCache> blockCache_;
    std::unordered_map<uint64_t, CacheEntry> cache_;
    std::list<uint64_t> lru_;
    std::mutex mutex_;
};

}
