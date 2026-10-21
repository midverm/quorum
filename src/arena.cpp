#include "arena.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <new>

namespace quorum {

using namespace std;

char* Arena::AllocateNewBlock(size_t size) {
    auto block = make_unique<char[]>(size);
    char* result = block.get();
    blocks_.push_back(std::move(block));
    memoryUsage_.fetch_add(size, memory_order_relaxed);
    return result;
}

char* Arena::Alloc(size_t size) {
    size = max<size_t>(size, 1);
    if (size > remaining_) return AllocateFallback(size);
    char* result = current_;
    current_ += size;
    remaining_ -= size;
    return result;
}

char* Arena::AllocateFallback(size_t size) {
    if (size > 1024) return AllocateNewBlock(size);
    current_ = AllocateNewBlock(4096);
    remaining_ = 4096;
    return Alloc(size);
}

char* Arena::AllocateAligned(size_t bytes) {
    constexpr size_t alignment = alignof(max_align_t);
    if (bytes > numeric_limits<size_t>::max() - alignment) throw bad_alloc();
    const auto address = reinterpret_cast<uintptr_t>(current_);
    const size_t padding = (alignment - address % alignment) % alignment;
    if (bytes + padding <= remaining_) {
        char* result = current_ + padding;
        current_ += bytes + padding;
        remaining_ -= bytes + padding;
        return result;
    }
    current_ = AllocateNewBlock(max<size_t>(4096, bytes + alignment));
    remaining_ = max<size_t>(4096, bytes + alignment);
    return AllocateAligned(bytes);
}

}
