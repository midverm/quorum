#include "bloom_filter.h"

#include "wal.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace quorum {

using namespace std;

uint64_t BloomFilter::Hash(string_view key) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char byte : key) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

void BloomFilter::Add(string_view key) { hashes_.push_back(Hash(key)); }

string BloomFilter::Finish() const {
    if (hashes_.size() > (numeric_limits<size_t>::max() - 7) / 10)
        throw length_error("Bloom filter size overflow");
    const uint64_t bitCount = (max<size_t>(64, hashes_.size() * 10) + 7) / 8 * 8;
    string filter;
    Put64(filter, bitCount);
    Put32(filter, 7);
    filter.resize(12 + bitCount / 8, '\0');
    for (uint64_t hash : hashes_) {
        const uint64_t delta = ((hash >> 17) | (hash << 47)) | 1;
        for (int i = 0; i < 7; ++i) {
            const uint64_t bit = hash % bitCount;
            auto& byte = filter[12 + bit / 8];
            byte = static_cast<char>(static_cast<uint8_t>(byte) | (1U << (bit % 8)));
            hash += delta;
        }
    }
    return filter;
}

bool BloomFilter::MayContain(string_view key, string_view filter) {
    uint64_t bitCount;
    uint32_t hashCount;
    if (!Take64(filter, bitCount) || !Take32(filter, hashCount) ||
        bitCount < 64 || bitCount % 8 != 0 || bitCount / 8 != filter.size() || hashCount != 7)
        return true;
    uint64_t hash = Hash(key);
    const uint64_t delta = ((hash >> 17) | (hash << 47)) | 1;
    for (uint32_t i = 0; i < hashCount; ++i) {
        const uint64_t bit = hash % bitCount;
        if ((static_cast<uint8_t>(filter[bit / 8]) & (1U << (bit % 8))) == 0) return false;
        hash += delta;
    }
    return true;
}

}
