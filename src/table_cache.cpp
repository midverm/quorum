#include "table_cache.h"

#include "sstable.h"
#include "version_set.h"

namespace quorum {

using namespace std;

Status TableCache::Get(const shared_ptr<TableFile>& file, shared_ptr<SSTableReader>& reader) {
    reader.reset();
    if (!file) return Status::InvalidArgument("missing SSTable file");
    const uint64_t number = file->metadata.number;
    {
        lock_guard<mutex> lock(mutex_);
        const auto found = cache_.find(number);
        if (found != cache_.end()) {
            lru_.splice(lru_.begin(), lru_, found->second.lru);
            reader = found->second.reader;
            return {};
        }
    }
    const auto& owner = file->fileOwner ? file->fileOwner : file;
    auto opened = make_shared<SSTableReader>(blockCache_, owner);
    Status status = opened->Open(file->path);
    if (!status.ok()) return status;
    shared_ptr<SSTableReader> evicted;
    {
        lock_guard<mutex> lock(mutex_);
        const auto found = cache_.find(number);
        if (found != cache_.end()) {
            lru_.splice(lru_.begin(), lru_, found->second.lru);
            reader = found->second.reader;
            return {};
        }
        reader = opened;
        // Old snapshots may open a file after compaction has evicted it.
        if (owner->obsolete.load(memory_order_relaxed)) return {};
        lru_.push_front(number);
        try {
            cache_.emplace(number, CacheEntry{std::move(opened), lru_.begin()});
        } catch (...) {
            lru_.pop_front();
            throw;
        }
        if (cache_.size() > kCapacity) {
            const auto oldest = cache_.find(lru_.back());
            evicted = std::move(oldest->second.reader);
            cache_.erase(oldest);
            lru_.pop_back();
        }
    }
    return {};
}

void TableCache::Evict(uint64_t fileNumber) {
    shared_ptr<SSTableReader> evicted;
    {
        lock_guard<mutex> lock(mutex_);
        const auto found = cache_.find(fileNumber);
        if (found == cache_.end()) return;
        evicted = std::move(found->second.reader);
        lru_.erase(found->second.lru);
        cache_.erase(found);
    }
}

}
