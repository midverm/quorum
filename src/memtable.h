#pragma once

#include "arena.h"
#include "skiplist.h"
#include "status.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

namespace quorum {

enum class ValueType : uint8_t { Deletion = 0, Value = 1 };
enum class GetState { Found, Absent, Deleted, Error };

struct GetResult {
    GetState state = GetState::Absent;
    std::string value;
    uint64_t sequence = 0;
    Status status;
};

std::string EncodeInternalKey(std::string_view key, uint64_t sequence, ValueType type);
bool ValidInternalKey(std::string_view key);
std::string_view UserKey(std::string_view internalKey);
uint64_t Sequence(std::string_view internalKey);
ValueType Type(std::string_view internalKey);
int CompareInternalKeys(std::string_view left, std::string_view right);

class MemTable {
public:
    MemTable() : list_(&arena_, CompareInternalKeys) {}
    void Add(uint64_t sequence, ValueType type, std::string_view key, std::string_view value);
    GetResult Get(std::string_view key, uint64_t snapshotSeq) const;
    std::size_t MemoryUsage() const { return arena_.MemoryUsage(); }
    void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }
    void Unref() {
        if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }
    SkipList::Iterator NewIterator() const { return list_.NewIterator(); }

private:
    ~MemTable() = default;
    Arena arena_;
    SkipList list_;
    std::atomic<unsigned> refs_{1};
};

}
