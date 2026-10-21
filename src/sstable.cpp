#include "sstable.h"

#include "wal.h"

#include <algorithm>
#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace quorum {

using namespace std;
namespace {

constexpr uint64_t kMagicNumber = 0x514f52554d535354ULL;
constexpr uint64_t kFooterSize = 24;

bool TakeEntry(string_view& bytes, string_view& key, string_view& value) {
    uint32_t length;
    if (!Take32(bytes, length) || length > bytes.size()) return false;
    key = bytes.substr(0, length);
    bytes.remove_prefix(length);
    if (!Take32(bytes, length) || length > bytes.size()) return false;
    value = bytes.substr(0, length);
    bytes.remove_prefix(length);
    return ValidInternalKey(key) && Sequence(key) != 0 &&
           (Type(key) != ValueType::Deletion || value.empty());
}

}

SSTableBuilder::SSTableBuilder(string path, uint64_t number, uint32_t level)
    : path_(std::move(path)) {
    metadata_.number = number;
    metadata_.level = level;
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd_ < 0) status_ = FileError("create SSTable " + path_);
    else created_ = true;
}

SSTableBuilder::~SSTableBuilder() {
    if (fd_ >= 0) ::close(fd_);
    if (created_ && !finished_) ::unlink(path_.c_str());
}

Status SSTableBuilder::Add(string_view internalKey, string_view value) {
    if (!status_.ok()) return status_;
    if (finished_) return Status::InvalidArgument("SSTable already finished");
    if (!ValidInternalKey(internalKey) || Sequence(internalKey) == 0 ||
        (Type(internalKey) == ValueType::Deletion && !value.empty()) ||
        internalKey.size() > kMaxRecordSize - 8 ||
        value.size() > kMaxRecordSize - 8 - internalKey.size() ||
        (!lastKey_.empty() && CompareInternalKeys(lastKey_, internalKey) >= 0))
        return status_ = Status::InvalidArgument("invalid or unordered SSTable entry");
    if (block_.size() + internalKey.size() + value.size() + 8 > kMaxRecordSize) {
        status_ = FlushBlock();
        if (!status_.ok()) return status_;
    }
    if (metadata_.smallest.empty()) metadata_.smallest.assign(internalKey);
    PutString(block_, internalKey);
    PutString(block_, value);
    lastKey_.assign(internalKey);
    if (block_.size() >= 4096) status_ = FlushBlock();
    return status_;
}

Status SSTableBuilder::FlushBlock() {
    if (!status_.ok()) return status_;
    if (finished_) return Status::InvalidArgument("SSTable already finished");
    if (block_.empty()) return {};
    status_ = AppendRecord(fd_, block_);
    if (!status_.ok()) return status_;
    const uint64_t size = block_.size() + 8;
    index_.push_back({lastKey_, offset_, size});
    offset_ += size;
    block_.clear();
    return {};
}

Status SSTableBuilder::Finish(FileMetadata& metadata) {
    if (!status_.ok()) return status_;
    if (finished_ || metadata_.smallest.empty())
        return Status::InvalidArgument("cannot finish empty or finished SSTable");
    status_ = FlushBlock();
    if (!status_.ok()) return status_;
    string indexBytes;
    Put32(indexBytes, static_cast<uint32_t>(index_.size()));
    for (const auto& block : index_) {
        if (indexBytes.size() + block.lastKey.size() + 20 > kMaxRecordSize)
            return status_ = Status::InvalidArgument("SSTable index exceeds 64 MiB");
        PutString(indexBytes, block.lastKey);
        Put64(indexBytes, block.offset);
        Put64(indexBytes, block.size);
    }
    status_ = AppendRecord(fd_, indexBytes);
    string footer;
    Put64(footer, offset_);
    Put64(footer, indexBytes.size() + 8);
    Put64(footer, kMagicNumber);
    if (status_.ok()) status_ = WriteAll(fd_, footer);
    if (status_.ok()) status_ = SyncFile(fd_);
    if (::close(fd_) != 0 && status_.ok()) status_ = FileError("close SSTable");
    fd_ = -1;
    if (status_.ok()) status_ = SyncDirectory(filesystem::path(path_).parent_path().string());
    if (!status_.ok()) return status_;
    metadata_.largest = lastKey_;
    metadata_.fileSize = offset_ + indexBytes.size() + 8 + kFooterSize;
    metadata = metadata_;
    finished_ = true;
    return {};
}

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) ::close(fd_);
}

Status SSTableReader::Open(const string& path) {
    if (fd_ >= 0) return Status::InvalidArgument("SSTable already open");
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return FileError("open SSTable " + path);
    fd_ = fd;
    struct stat info{};
    Status status;
    if (::fstat(fd, &info) != 0) status = FileError("stat SSTable");
    if (status.ok() && info.st_size < static_cast<off_t>(kFooterSize))
        status = Status::Corruption("short SSTable footer");
    string footer;
    if (status.ok()) status = ReadAt(fd, info.st_size - kFooterSize, kFooterSize, footer);
    uint64_t indexOffset = 0, indexSize = 0, magic = 0;
    if (status.ok()) {
        string_view view(footer);
        Take64(view, indexOffset);
        Take64(view, indexSize);
        Take64(view, magic);
        const uint64_t dataEnd = static_cast<uint64_t>(info.st_size) - kFooterSize;
        if (magic != kMagicNumber || indexSize < 8 || indexSize > kMaxRecordSize + 8ULL ||
            indexOffset > dataEnd || indexSize != dataEnd - indexOffset)
            status = Status::Corruption("invalid SSTable footer");
    }
    string indexBytes;
    if (status.ok()) {
        uint64_t offset = indexOffset;
        bool complete;
        status = ReadRecord(fd, indexOffset + indexSize, offset, indexBytes, complete);
        if (status.ok() && (!complete || offset != indexOffset + indexSize))
            status = Status::Corruption("invalid SSTable index record");
    }
    vector<BlockIndex> blocks;
    if (status.ok()) {
        string_view view(indexBytes);
        uint32_t count = 0;
        if (!Take32(view, count) || count == 0 || count > view.size() / 29)
            status = Status::Corruption("invalid SSTable block count");
        uint64_t nextOffset = 0;
        for (uint32_t i = 0; i < count && status.ok(); ++i) {
            BlockIndex block;
            if (!TakeString(view, block.lastKey) || !ValidInternalKey(block.lastKey) ||
                Sequence(block.lastKey) == 0 || !Take64(view, block.offset) ||
                !Take64(view, block.size) || block.offset != nextOffset ||
                block.size < 8 || block.size > kMaxRecordSize + 8ULL ||
                block.offset > indexOffset || block.size > indexOffset - block.offset ||
                (!blocks.empty() && CompareInternalKeys(blocks.back().lastKey, block.lastKey) >= 0)) {
                status = Status::Corruption("invalid SSTable block index");
                break;
            }
            nextOffset += block.size;
            blocks.push_back(std::move(block));
        }
        if (status.ok() && (!view.empty() || nextOffset != indexOffset))
            status = Status::Corruption("invalid SSTable index bounds");
    }
    if (!status.ok()) {
        ::close(fd);
        fd_ = -1;
        return status;
    }
    index_ = std::move(blocks);
    return {};
}

Status SSTableReader::ReadBlock(size_t blockNumber, string& block) const {
    if (fd_ < 0) return Status::Closed();
    const auto& entry = index_[blockNumber];
    uint64_t offset = entry.offset;
    bool complete;
    Status status = ReadRecord(fd_, entry.offset + entry.size, offset, block, complete);
    if (!status.ok()) return status;
    if (!complete || offset != entry.offset + entry.size || block.empty())
        return Status::Corruption("invalid SSTable data block");
    string_view remaining(block), key, value, previous;
    while (!remaining.empty()) {
        if (!TakeEntry(remaining, key, value) ||
            (!previous.empty() && CompareInternalKeys(previous, key) >= 0) ||
            (previous.empty() && blockNumber > 0 &&
             CompareInternalKeys(index_[blockNumber - 1].lastKey, key) >= 0))
            return Status::Corruption("invalid SSTable data entry");
        previous = key;
    }
    if (previous != entry.lastKey) return Status::Corruption("SSTable block boundary mismatch");
    return {};
}

GetResult SSTableReader::Get(string_view key, uint64_t snapshotSeq) const {
    if (fd_ < 0) return {GetState::Error, {}, 0, Status::Closed()};
    const auto lookup = EncodeInternalKey(key, snapshotSeq, ValueType::Value);
    const auto entry = lower_bound(index_.begin(), index_.end(), lookup,
        [](const BlockIndex& block, string_view target) {
            return CompareInternalKeys(block.lastKey, target) < 0;
        });
    if (entry == index_.end()) return {};
    string block;
    Status status = ReadBlock(static_cast<size_t>(entry - index_.begin()), block);
    if (!status.ok()) return {GetState::Error, {}, 0, status};
    string_view remaining(block), internalKey, value;
    while (!remaining.empty()) {
        if (!TakeEntry(remaining, internalKey, value))
            return {GetState::Error, {}, 0, Status::Corruption("invalid SSTable entry")};
        if (CompareInternalKeys(internalKey, lookup) < 0) continue;
        if (UserKey(internalKey) != key) return {};
        return {Type(internalKey) == ValueType::Deletion ? GetState::Deleted : GetState::Found,
                string(value), Sequence(internalKey), {}};
    }
    return {};
}

void SSTableReader::Iterator::SeekToFirst() {
    blockNumber_ = 0;
    status_ = {};
    valid_ = false;
    if (reader_->fd_ < 0) {
        status_ = Status::Closed();
        return;
    }
    LoadBlock();
}

void SSTableReader::Iterator::LoadBlock() {
    valid_ = false;
    if (blockNumber_ >= reader_->index_.size()) return;
    status_ = reader_->ReadBlock(blockNumber_, block_);
    if (!status_.ok()) return;
    remaining_ = block_;
    ReadEntry();
}

void SSTableReader::Iterator::ReadEntry() {
    valid_ = TakeEntry(remaining_, key_, value_);
    if (!valid_) status_ = Status::Corruption("invalid SSTable iterator entry");
}

void SSTableReader::Iterator::Next() {
    if (!valid_) return;
    if (!remaining_.empty()) ReadEntry();
    else {
        ++blockNumber_;
        LoadBlock();
    }
}

}
