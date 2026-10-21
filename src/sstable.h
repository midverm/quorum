#pragma once

#include "memtable.h"
#include "status.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace quorum {

struct FileMetadata {
    uint64_t number = 0;
    uint32_t level = 0;
    uint64_t fileSize = 0;
    std::string smallest;
    std::string largest;
};

struct BlockIndex {
    std::string lastKey;
    uint64_t offset = 0;
    uint64_t size = 0;
};

class SSTableBuilder {
public:
    SSTableBuilder(std::string path, uint64_t number, uint32_t level = 0);
    ~SSTableBuilder();
    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;
    Status Add(std::string_view internalKey, std::string_view value);
    Status FlushBlock();
    Status Finish(FileMetadata& metadata);
    uint64_t FileSize() const { return offset_ + block_.size(); }

private:
    std::string path_;
    int fd_ = -1;
    bool created_ = false;
    bool finished_ = false;
    Status status_;
    FileMetadata metadata_;
    uint64_t offset_ = 0;
    std::string block_;
    std::string lastKey_;
    std::vector<BlockIndex> index_;
};

class SSTableReader {
public:
    class Iterator {
    public:
        explicit Iterator(const SSTableReader* reader) : reader_(reader) {}
        void SeekToFirst();
        void Next();
        bool Valid() const { return valid_; }
        std::string_view key() const { return key_; }
        std::string_view value() const { return value_; }
        const Status& status() const { return status_; }

    private:
        void LoadBlock();
        void ReadEntry();
        const SSTableReader* reader_;
        std::size_t blockNumber_ = 0;
        std::string block_;
        std::string_view remaining_;
        std::string_view key_;
        std::string_view value_;
        bool valid_ = false;
        Status status_;
    };

    SSTableReader() = default;
    ~SSTableReader();
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    Status Open(const std::string& path);
    GetResult Get(std::string_view key, uint64_t snapshotSeq) const;
    std::unique_ptr<Iterator> NewIterator() const { return std::make_unique<Iterator>(this); }

private:
    Status ReadBlock(std::size_t blockNumber, std::string& block) const;
    int fd_ = -1;
    std::vector<BlockIndex> index_;
};

}
