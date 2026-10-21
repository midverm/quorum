#include "memtable.h"

#include <cstring>

namespace quorum {

using namespace std;

string EncodeInternalKey(string_view key, uint64_t sequence, ValueType type) {
    string result(key);
    for (int i = 0; i < 8; ++i) result.push_back(static_cast<char>(sequence >> (i * 8)));
    result.push_back(static_cast<char>(type));
    return result;
}

bool ValidInternalKey(string_view key) {
    return key.size() >= 9 && static_cast<uint8_t>(key.back()) <= 1;
}

string_view UserKey(string_view key) { return key.substr(0, key.size() - 9); }

uint64_t Sequence(string_view key) {
    uint64_t result = 0;
    for (int i = 0; i < 8; ++i)
        result |= static_cast<uint64_t>(static_cast<uint8_t>(key[key.size() - 9 + i])) << (i * 8);
    return result;
}

ValueType Type(string_view key) { return static_cast<ValueType>(key.back()); }

int CompareInternalKeys(string_view left, string_view right) {
    const int comparison = UserKey(left).compare(UserKey(right));
    if (comparison != 0) return comparison;
    const uint64_t a = Sequence(left), b = Sequence(right);
    if (a != b) return a > b ? -1 : 1;
    return static_cast<int>(Type(right)) - static_cast<int>(Type(left));
}

void MemTable::Add(uint64_t sequence, ValueType type, string_view key,
                   string_view value) {
    const string internalKey = EncodeInternalKey(key, sequence, type);
    char* bytes = arena_.Alloc(internalKey.size() + value.size());
    memcpy(bytes, internalKey.data(), internalKey.size());
    if (!value.empty()) memcpy(bytes + internalKey.size(), value.data(), value.size());
    list_.Insert({bytes, internalKey.size()}, {bytes + internalKey.size(), value.size()});
}

GetResult MemTable::Get(string_view key, uint64_t snapshotSeq) const {
    auto iterator = NewIterator();
    iterator.Seek(EncodeInternalKey(key, snapshotSeq, ValueType::Value));
    if (!iterator.Valid() || UserKey(iterator.key()) != key) return {};
    return {Type(iterator.key()) == ValueType::Deletion ? GetState::Deleted : GetState::Found,
            string(iterator.value()), Sequence(iterator.key()), {}};
}

}
