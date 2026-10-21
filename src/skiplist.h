#pragma once

#include "arena.h"

#include <atomic>
#include <cstdint>
#include <string_view>

namespace quorum {

class SkipList {
    static constexpr int branching = 4;
    static constexpr int max_height = 8;
    struct Node {
        Node(std::string_view key, std::string_view value);
        std::string_view key;
        std::string_view value;
        std::atomic<Node*> next[max_height];
    };

public:
    using Comparator = int (*)(std::string_view, std::string_view);
    class Iterator {
    public:
        explicit Iterator(const SkipList* list) : list_(list) {}
        bool Valid() const { return node_ != nullptr; }
        void SeekToFirst();
        void Seek(std::string_view key);
        void Next();
        std::string_view key() const { return node_->key; }
        std::string_view value() const { return node_->value; }

    private:
        const SkipList* list_;
        Node* node_ = nullptr;
    };

    SkipList(Arena* arena, Comparator compare);
    void Insert(std::string_view key, std::string_view value);
    Iterator NewIterator() const { return Iterator(this); }

private:
    int RandomHeight();
    Node* Find(std::string_view key, Node** previous) const;
    Arena* arena_;
    Comparator compare_;
    Node* head_;
    uint32_t random_seed = 0x9e3779b9;
};

}
