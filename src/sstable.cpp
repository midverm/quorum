#include "sstable.h"

#include "block_cache.h"
#include "version_set.h"
#include "wal.h"

#include <algorithm>
#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace quorum {

using namespace std;
namespace {

constexpr uint64_t kMagicNumber = 0x514f52554d535333ULL;
constexpr uint64_t kFooterSize = 40;
constexpr uint32_t kRestartInterval = 16;

bool TakeEntry(string_view& bytes, string& key, string_view& value) {
    uint32_t shared, suffix, valueSize;
    if (!Take32(bytes, shared) || !Take32(bytes, suffix) || !Take32(bytes, valueSize) ||
        shared > key.size() || suffix > bytes.size() || valueSize > bytes.size() - suffix)
        return false;
    key.resize(shared);
    key.append(bytes.data(), suffix);
    bytes.remove_prefix(suffix);
    value = bytes.substr(0, valueSize);
    bytes.remove_prefix(valueSize);
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
        (!lastKey_.empty() && CompareInternalKeys(lastKey_, internalKey) > 0))
        return status_ = Status::InvalidArgument("invalid or unordered SSTable entry");
    uint32_t sharedLen = 0;
    bool restart = entriesInBlock_ % kRestartInterval == 0;
    if (!restart) {
        while (sharedLen < internalKey.size() && sharedLen < previousKey_.size() &&
               internalKey[sharedLen] == previousKey_[sharedLen])
            ++sharedLen;
    }
    size_t entrySize = 12 + internalKey.size() - sharedLen + value.size();
    size_t restartSize = (restarts_.size() + (restart ? 1 : 0)) * 4 + 4;
    if (block_.size() + entrySize + restartSize > kMaxRecordSize) {
        status_ = FlushBlock();
        if (!status_.ok()) return status_;
        sharedLen = 0;
        restart = true;
        entrySize = 12 + internalKey.size() + value.size();
        restartSize = 8;
    }
    if (entrySize + restartSize > kMaxRecordSize)
        return status_ = Status::InvalidArgument("SSTable entry exceeds data block size limit");
    if (metadata_.smallest.empty()) metadata_.smallest.assign(internalKey);
    if (restart) restarts_.push_back(static_cast<uint32_t>(block_.size()));
    Put32(block_, sharedLen);
    Put32(block_, static_cast<uint32_t>(internalKey.size() - sharedLen));
    Put32(block_, static_cast<uint32_t>(value.size()));
    block_.append(internalKey.substr(sharedLen));
    block_.append(value);
    previousKey_.assign(internalKey);
    lastKey_.assign(internalKey);
    ++entriesInBlock_;
    if (block_.size() + restarts_.size() * 4 + 4 >= 4096) status_ = FlushBlock();
    if (status_.ok()) bloom_.Add(UserKey(internalKey));
    return status_;
}

Status SSTableBuilder::FlushBlock() {
    if (!status_.ok()) return status_;
    if (finished_) return Status::InvalidArgument("SSTable already finished");
    if (block_.empty()) return {};
    for (uint32_t offset : restarts_) Put32(block_, offset);
    Put32(block_, static_cast<uint32_t>(restarts_.size()));
    status_ = AppendRecord(fd_, block_);
    if (!status_.ok()) return status_;
    const uint64_t size = block_.size() + 8;
    index_.push_back({lastKey_, offset_, size});
    offset_ += size;
    block_.clear();
    previousKey_.clear();
    restarts_.clear();
    entriesInBlock_ = 0;
    return {};
}

Status SSTableBuilder::Finish(FileMetadata& metadata) {
    if (!status_.ok()) return status_;
    if (finished_ || metadata_.smallest.empty())
        return Status::InvalidArgument("cannot finish empty or finished SSTable");
    status_ = FlushBlock();
    if (!status_.ok()) return status_;
    const string filterBytes = bloom_.Finish();
    const uint64_t filterOffset = offset_;
    const uint64_t filterSize = filterBytes.size() + 8;
    status_ = AppendRecord(fd_, filterBytes);
    if (!status_.ok()) return status_;
    offset_ += filterSize;
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
    Put64(footer, filterOffset);
    Put64(footer, filterSize);
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
    uint64_t filterOffset = 0, filterSize = 0;
    if (status.ok()) {
        string_view view(footer);
        if (!Take64(view, filterOffset) || !Take64(view, filterSize) ||
            !Take64(view, indexOffset) || !Take64(view, indexSize) ||
            !Take64(view, magic) || magic != kMagicNumber)
            status = Status::Corruption("invalid SSTable footer");
    }
    if (status.ok()) {
        const uint64_t dataEnd = static_cast<uint64_t>(info.st_size) - kFooterSize;
        if (indexSize < 8 || indexSize > kMaxRecordSize + 8ULL ||
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
    uint64_t dataEnd = 0;
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
                (!blocks.empty() && CompareInternalKeys(blocks.back().lastKey, block.lastKey) > 0)) {
                status = Status::Corruption("invalid SSTable block index");
                break;
            }
            nextOffset += block.size;
            blocks.push_back(std::move(block));
        }
        if (status.ok() && !view.empty())
            status = Status::Corruption("invalid SSTable index bounds");
        dataEnd = nextOffset;
    }
    string filter;
    if (status.ok() && filterOffset == dataEnd &&
        filterOffset <= indexOffset && filterSize >= 8 && filterSize <= kMaxRecordSize + 8ULL &&
        filterSize == indexOffset - filterOffset) {
        uint64_t offset = filterOffset;
        bool complete;
        Status filterStatus = ReadRecord(fd, indexOffset, offset, filter, complete);
        if (!filterStatus.ok() || !complete || offset != indexOffset) filter.clear();
    }
    if (!status.ok()) {
        ::close(fd);
        fd_ = -1;
        return status;
    }
    index_ = std::move(blocks);
    bloomFilter_ = std::move(filter);
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
    return {};
}

GetResult SSTableReader::Get(string_view key, uint64_t snapshotSeq) const {
    if (fd_ < 0) return {GetState::Error, {}, 0, Status::Closed()};
    if (!bloomFilter_.empty() && !BloomFilter::MayContain(key, bloomFilter_)) return {};
    Iterator iterator(this);
    iterator.Seek(EncodeInternalKey(key, snapshotSeq, ValueType::Value));
    if (!iterator.status().ok()) return {GetState::Error, {}, 0, iterator.status()};
    if (!iterator.Valid() || UserKey(iterator.key()) != key) return {};
    return {Type(iterator.key()) == ValueType::Deletion ? GetState::Deleted : GetState::Found,
            string(iterator.value()), Sequence(iterator.key()), {}};
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
    remaining_ = {};
    value_ = {};
    block_.reset();
    if (blockNumber_ >= reader_->index_.size()) return;
    const auto& index = reader_->index_[blockNumber_];
    const auto& cache = reader_->blockCache_;
    const auto& file = reader_->file_;
    const BlockKey cacheKey{file ? file->metadata.number : 0, index.offset};
    if (cache && file) block_ = cache->Get(cacheKey);
    const bool cached = block_ != nullptr;
    if (!cached) {
        auto bytes = make_shared<string>();
        status_ = reader_->ReadBlock(blockNumber_, *bytes);
        if (!status_.ok()) return;
        block_ = std::move(bytes);
    }
    previousKey_.clear();
    restarts_.clear();
    entriesSinceRestart_ = 0;
    entriesEnd_ = block_->size();
    uint32_t count;
    string_view tail(*block_);
    if (tail.size() < 8) {
        status_ = Status::Corruption("short SSTable restart metadata");
        return;
    }
    tail.remove_prefix(tail.size() - 4);
    if (!Take32(tail, count) || count == 0 || count > (block_->size() - 4) / 4) {
        status_ = Status::Corruption("invalid SSTable restart count");
        return;
    }
    entriesEnd_ = block_->size() - 4 - static_cast<size_t>(count) * 4;
    if (count > entriesEnd_ / 21) {
        status_ = Status::Corruption("invalid SSTable restart count");
        return;
    }
    tail = string_view(*block_).substr(entriesEnd_, static_cast<size_t>(count) * 4);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t offset;
        if (!Take32(tail, offset) || offset >= entriesEnd_ || (i == 0 && offset != 0) ||
            (!restarts_.empty() && offset <= restarts_.back())) {
            status_ = Status::Corruption("invalid SSTable restart offset");
            return;
        }
        restarts_.push_back(offset);
    }
    string previous;
    for (size_t i = 0; i < restarts_.size(); ++i) {
        const size_t end = i + 1 < restarts_.size() ? restarts_[i + 1] : entriesEnd_;
        string_view entry = string_view(*block_).substr(restarts_[i], end - restarts_[i]);
        string key;
        string_view value;
        if (!TakeEntry(entry, key, value) ||
            (!previous.empty() && CompareInternalKeys(previous, key) > 0) ||
            (blockNumber_ > 0 && CompareInternalKeys(reader_->index_[blockNumber_ - 1].lastKey, key) > 0) ||
            CompareInternalKeys(key, reader_->index_[blockNumber_].lastKey) > 0) {
            status_ = Status::Corruption("invalid SSTable restart entry");
            return;
        }
        previous = std::move(key);
    }
    remaining_ = string_view(*block_).substr(0, entriesEnd_);
    ReadEntry();
    if (!cached && cache && file) {
        // Validate every entry before publishing immutable bytes to other readers.
        while (Valid() && !remaining_.empty()) ReadEntry();
        if (!status_.ok()) return;
        if (!file->obsolete.load(memory_order_relaxed)) cache->Insert(cacheKey, block_);
        // Publication can race the insert; obsolete files must not repopulate the cache.
        if (file->obsolete.load(memory_order_relaxed)) cache->EvictFile(cacheKey.fileNumber);
        previousKey_.clear();
        entriesSinceRestart_ = 0;
        remaining_ = string_view(*block_).substr(0, entriesEnd_);
        ReadEntry();
    }
}

void SSTableReader::Iterator::ReadEntry() {
    valid_ = false;
    const size_t position = entriesEnd_ - remaining_.size();
    auto restart = lower_bound(restarts_.begin(), restarts_.end(), position);
    const bool atRestart = restart != restarts_.end() && *restart == position;
    uint32_t shared;
    string_view header = remaining_;
    if (atRestart != (entriesSinceRestart_ == 0) || !Take32(header, shared) ||
        (atRestart && shared != 0)) {
        status_ = Status::Corruption("invalid SSTable restart boundary");
        return;
    }
    if (atRestart) ++restart;
    key_ = previousKey_;
    if (!TakeEntry(remaining_, key_, value_) ||
        (!previousKey_.empty() && CompareInternalKeys(previousKey_, key_) > 0) ||
        (previousKey_.empty() && blockNumber_ > 0 &&
         CompareInternalKeys(reader_->index_[blockNumber_ - 1].lastKey, key_) > 0) ||
        CompareInternalKeys(key_, reader_->index_[blockNumber_].lastKey) > 0 ||
        (restart != restarts_.end() && *restart < entriesEnd_ - remaining_.size()) ||
        (remaining_.empty() && key_ != reader_->index_[blockNumber_].lastKey)) {
        status_ = Status::Corruption("invalid SSTable iterator entry");
        return;
    }
    previousKey_ = key_;
    entriesSinceRestart_ = (entriesSinceRestart_ + 1) % kRestartInterval;
    valid_ = true;
}

void SSTableReader::Iterator::Seek(string_view target) {
    const string lookup(target);
    status_ = {};
    valid_ = false;
    if (reader_->fd_ < 0) {
        status_ = Status::Closed();
        return;
    }
    if (!ValidInternalKey(lookup)) {
        status_ = Status::InvalidArgument("invalid SSTable lookup key");
        return;
    }
    const auto block = lower_bound(reader_->index_.begin(), reader_->index_.end(), lookup,
        [](const BlockIndex& entry, string_view key) {
            return CompareInternalKeys(entry.lastKey, key) < 0;
        });
    blockNumber_ = static_cast<size_t>(block - reader_->index_.begin());
    LoadBlock();
    if (!Valid()) return;
    size_t low = 0, high = restarts_.size();
    while (low < high) {
        const size_t middle = low + (high - low) / 2;
        string_view entry = string_view(*block_).substr(restarts_[middle], entriesEnd_ - restarts_[middle]);
        string key;
        string_view value;
        if (!TakeEntry(entry, key, value)) {
            valid_ = false;
            status_ = Status::Corruption("invalid SSTable restart entry");
            return;
        }
        if (CompareInternalKeys(key, lookup) < 0) low = middle + 1;
        else high = middle;
    }
    const size_t offset = restarts_[low == 0 ? 0 : low - 1];
    remaining_ = string_view(*block_).substr(offset, entriesEnd_ - offset);
    previousKey_.clear();
    entriesSinceRestart_ = 0;
    ReadEntry();
    while (Valid() && CompareInternalKeys(key_, lookup) < 0) Next();
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
