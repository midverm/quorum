#include "skiplist.h"

#include <new>

namespace quorum {

using namespace std;

SkipList::Node::Node(string_view keyValue, string_view storedValue)
    : key(keyValue), value(storedValue) {
    for (auto& link : next) link.store(nullptr, memory_order_relaxed);
}

SkipList::SkipList(Arena* arena, Comparator compare) : arena_(arena), compare_(compare) {
    head_ = new (arena_->AllocateAligned(sizeof(Node))) Node({}, {});
}

SkipList::Node* SkipList::Find(string_view key, Node** previous) const {
    Node* current = head_;
    for (int level = max_height - 1; level >= 0; --level) {
        Node* next = current->next[level].load(memory_order_acquire);
        while (next && compare_(next->key, key) < 0) {
            current = next;
            next = current->next[level].load(memory_order_acquire);
        }
        if (previous) previous[level] = current;
    }
    return current->next[0].load(memory_order_acquire);
}

int SkipList::RandomHeight() {
    int height = 1;
    while (height < max_height) {
        random_seed ^= random_seed << 13;
        random_seed ^= random_seed >> 17;
        random_seed ^= random_seed << 5;
        if (random_seed % branching != 0) break;
        ++height;
    }
    return height;
}

void SkipList::Insert(string_view key, string_view value) {
    Node* previous[max_height];
    Node* existing = Find(key, previous);
    if (existing && compare_(existing->key, key) == 0) return;
    const int height = RandomHeight();
    auto* node = new (arena_->AllocateAligned(sizeof(Node))) Node(key, value);
    for (int level = 0; level < height; ++level) {
        node->next[level].store(previous[level]->next[level].load(memory_order_relaxed),
                                memory_order_relaxed);
        previous[level]->next[level].store(node, memory_order_release);
    }
}

void SkipList::Iterator::SeekToFirst() {
    node_ = list_->head_->next[0].load(memory_order_acquire);
}

void SkipList::Iterator::Seek(string_view key) { node_ = list_->Find(key, nullptr); }

void SkipList::Iterator::Next() {
    if (node_) node_ = node_->next[0].load(memory_order_acquire);
}

}
