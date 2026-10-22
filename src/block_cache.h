#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace quorum {

struct BlockKey {
    uint64_t fileNumber;
    uint64_t blockOffset;

    bool operator==(const BlockKey& other) const {
        return fileNumber == other.fileNumber && blockOffset == other.blockOffset;
    }
};

struct BlockKeyHash {
    std::size_t operator()(const BlockKey& key) const {
        const auto first = std::hash<uint64_t>{}(key.fileNumber);
        return first ^ (std::hash<uint64_t>{}(key.blockOffset) + 0x9e3779b9 +
                        (first << 6) + (first >> 2));
    }
};

class BlockCache {
public:
    explicit BlockCache(std::size_t capacity = 32 * 1024 * 1024) : capacity_(capacity) {}
    std::shared_ptr<const std::string> Get(const BlockKey& key);
    void Insert(const BlockKey& key, std::shared_ptr<const std::string> block);
    void EvictFile(uint64_t fileNumber);

private:
    struct CacheEntry {
        std::shared_ptr<const std::string> block;
        std::list<BlockKey>::iterator lru;
        std::size_t bytes;
    };
    const std::size_t capacity_;
    std::unordered_map<BlockKey, CacheEntry, BlockKeyHash> cache_;
    std::list<BlockKey> lru_;
    std::mutex mutex_;
    std::size_t currentBytes_ = 0;
};

}
