#include "block_cache.h"

namespace quorum {

using namespace std;

shared_ptr<const string> BlockCache::Get(const BlockKey& key) {
    lock_guard<mutex> lock(mutex_);
    const auto found = cache_.find(key);
    if (found == cache_.end()) return nullptr;
    lru_.splice(lru_.begin(), lru_, found->second.lru);
    return found->second.block;
}

void BlockCache::Insert(const BlockKey& key, shared_ptr<const string> block) {
    if (!block || capacity_ == 0 || block->size() > capacity_) return;
    const size_t bytes = block->size();
    lock_guard<mutex> lock(mutex_);
    const auto found = cache_.find(key);
    if (found != cache_.end()) {
        currentBytes_ -= found->second.bytes;
        found->second.block = std::move(block);
        found->second.bytes = bytes;
        lru_.splice(lru_.begin(), lru_, found->second.lru);
    } else {
        lru_.push_front(key);
        try {
            cache_.emplace(key, CacheEntry{std::move(block), lru_.begin(), bytes});
        } catch (...) {
            lru_.pop_front();
            throw;
        }
    }
    currentBytes_ += bytes;
    while (currentBytes_ > capacity_) {
        const auto oldest = cache_.find(lru_.back());
        currentBytes_ -= oldest->second.bytes;
        cache_.erase(oldest);
        lru_.pop_back();
    }
}

void BlockCache::EvictFile(uint64_t fileNumber) {
    lock_guard<mutex> lock(mutex_);
    for (auto it = cache_.begin(); it != cache_.end();) {
        if (it->first.fileNumber != fileNumber) {
            ++it;
            continue;
        }
        currentBytes_ -= it->second.bytes;
        lru_.erase(it->second.lru);
        it = cache_.erase(it);
    }
}

}
