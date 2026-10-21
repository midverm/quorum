#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace quorum {

class Arena {
public:
    char* Alloc(std::size_t size);
    char* AllocateAligned(std::size_t bytes);
    std::size_t MemoryUsage() const { return memoryUsage_.load(std::memory_order_relaxed); }

private:
    char* AllocateFallback(std::size_t size);
    char* AllocateNewBlock(std::size_t size);
    std::vector<std::unique_ptr<char[]>> blocks_;
    char* current_ = nullptr;
    std::size_t remaining_ = 0;
    std::atomic<std::size_t> memoryUsage_{0};
};

}
