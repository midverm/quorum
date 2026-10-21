#include "db.h"

#include <algorithm>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <queue>
#include <set>
#include <system_error>
#include <sys/file.h>
#include <unistd.h>

namespace quorum {

using namespace std;

DB::~DB() { Close(); }

Status DB::Open() {
    lock_guard<mutex> lifecycle(lifecycleMutex_);
    {
        lock_guard<mutex> lock(dbMutex_);
        if (opened_) return Status::InvalidArgument("database already open");
        shuttingDown_ = true;
    }
    if (path_.empty()) return Status::InvalidArgument("database path is empty");
    backgroundError_ = {};
    closeStatus_ = {};
    versions_ = VersionSet(path_);
    error_code error;
    const bool created = filesystem::create_directories(path_, error);
    if (error) return Status::IOError("create database directory: " + error.message());
    if (created) {
        auto parent = filesystem::path(path_).parent_path();
        Status status = SyncDirectory(parent.empty() ? "." : parent.string());
        if (!status.ok()) return status;
    }
    lockFd_ = ::open((path_ + "/LOCK").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (lockFd_ < 0) return FileError("open database lock");
    if (::flock(lockFd_, LOCK_EX | LOCK_NB) != 0) {
        Status status = FileError("lock database");
        ::close(lockFd_);
        lockFd_ = -1;
        return status;
    }
    Status status;
    try {
        status = Recover();
        if (status.ok()) {
            lock_guard<mutex> lock(dbMutex_);
            shuttingDown_ = false;
            flushThread_ = thread(&DB::FlushWorker, this);
            opened_ = true;
        }
    } catch (const exception& exception) {
        status = Status::IOError(exception.what());
    }
    if (!status.ok()) ReleaseResources();
    return status;
}

Status DB::Recover() {
    error_code error;
    vector<uint64_t> logs;
    uint64_t largestFile = 0;
    bool hasTables = false;
    for (filesystem::directory_iterator it(path_, error), end; !error && it != end; it.increment(error)) {
        const string name = it->path().filename().string();
        uint64_t number;
        const bool isWal = ParseFileName(name, ".wal", number);
        if (isWal || ParseFileName(name, ".sst", number)) {
            if (name != FileName(number, isWal ? ".wal" : ".sst"))
                return Status::Corruption("noncanonical database file name");
            largestFile = max(largestFile, number);
            if (isWal) logs.push_back(number);
            else hasTables = true;
        }
    }
    if (error) return Status::IOError("list database files: " + error.message());
    const bool hasManifest = filesystem::exists(path_ + "/MANIFEST", error);
    if (error) return Status::IOError("find MANIFEST: " + error.message());
    vector<VersionEdit> edits;
    Status status;
    if (hasManifest) status = manifest_.Recover(path_ + "/MANIFEST", edits);
    else {
        if (hasTables || !logs.empty()) return Status::Corruption("database MANIFEST is missing");
        status = manifest_.Create(path_ + "/MANIFEST");
    }
    if (!status.ok()) return status;
    if (edits.empty()) {
        if (hasTables || !logs.empty()) return Status::Corruption("database MANIFEST is empty");
        VersionEdit initial;
        status = manifest_.AppendAndSync(initial);
        if (!status.ok()) return status;
        edits.push_back(initial);
    }
    lastSequence_ = 0;
    oldestRequiredWal_ = 1;
    for (const auto& edit : edits) {
        if (edit.oldestRequiredWal > edit.nextFileNumber)
            return Status::Corruption("invalid MANIFEST WAL boundary");
        shared_ptr<Version> next;
        status = versions_.BuildNext(edit, next);
        if (!status.ok()) return status;
        versions_.Publish(std::move(next));
        lastSequence_ = edit.lastSequence;
        oldestRequiredWal_ = edit.oldestRequiredWal;
        versions_.SetNextFileNumber(edit.nextFileNumber);
    }
    if (largestFile == numeric_limits<uint64_t>::max())
        return Status::Corruption("file numbers exhausted");
    versions_.SetNextFileNumber(max(versions_.NextFileNumber(), largestFile + 1));
    for (const auto& file : versions_.Current()->files) {
        const auto size = filesystem::file_size(file->path, error);
        if (error) return Status::IOError("stat live SSTable: " + error.message());
        if (size != file->metadata.fileSize) return Status::Corruption("SSTable size mismatch");
        SSTableReader reader;
        status = reader.Open(file->path);
        if (!status.ok()) return status;
    }
    sort(logs.begin(), logs.end());
    if (oldestRequiredWal_ < edits.back().nextFileNumber &&
        !binary_search(logs.begin(), logs.end(), oldestRequiredWal_))
        return Status::Corruption("required WAL is missing");
    mem_ = new MemTable;
    memOldestWal_ = oldestRequiredWal_;
    for (uint64_t number : logs) {
        if (number < oldestRequiredWal_) continue;
        status = WAL::Replay(path_ + "/" + FileName(number, ".wal"), mem_, lastSequence_, [&] {
            return mem_->MemoryUsage() >= kMemtableLimit ? FlushRecoveredMemTable() : Status{};
        });
        if (!status.ok()) return status;
    }
    currentWalId_ = versions_.NewFileNumber();
    if (currentWalId_ == 0) return Status::IOError("file numbers exhausted");
    wal_ = make_unique<WAL>();
    status = wal_->Create(path_ + "/" + FileName(currentWalId_, ".wal"));
    if (!status.ok()) return status;
    set<uint64_t> liveTables;
    for (const auto& file : versions_.Current()->files) liveTables.insert(file->metadata.number);
    for (filesystem::directory_iterator it(path_, error), end; !error && it != end; it.increment(error)) {
        uint64_t number;
        if (ParseFileName(it->path().filename().string(), ".sst", number) && !liveTables.count(number)) {
            filesystem::remove(it->path(), error);
            if (error) break;
        }
    }
    if (error) return Status::IOError("remove orphan SSTable: " + error.message());
    return DeleteObsoleteWALs(oldestRequiredWal_);
}

Status DB::Put(string_view key, string_view value, bool sync) {
    return Write(ValueType::Value, key, value, sync);
}

Status DB::Delete(string_view key, bool sync) {
    return Write(ValueType::Deletion, key, {}, sync);
}

Status DB::Write(ValueType type, string_view key, string_view value, bool sync) {
    if (key.size() > kMaxRecordSize - 17 || value.size() > kMaxRecordSize - 17 - key.size())
        return Status::InvalidArgument("entry exceeds 64 MiB");
    lock_guard<mutex> writer(writerMutex_);
    unique_lock<mutex> lock(dbMutex_);
    if (!opened_ || shuttingDown_) return Status::Closed();
    if (!backgroundError_.ok()) return backgroundError_;
    if (lastSequence_ == numeric_limits<uint64_t>::max())
        return Status::InvalidArgument("sequence numbers exhausted");
    ++activeOperations_;
    Status status;
    try {
        status = MakeRoomForWrite(lock);
    } catch (const exception& exception) {
        if (!lock.owns_lock()) lock.lock();
        status = Status::IOError(exception.what());
    }
    if (!status.ok()) {
        --activeOperations_;
        operationsCV_.notify_all();
        return status;
    }
    MemTable* mem = mem_;
    const uint64_t sequence = lastSequence_ + 1;
    lock.unlock();
    try {
        status = wal_->Append(sequence, type, key, value);
        if (status.ok() && sync) status = wal_->Sync();
        if (status.ok()) mem->Add(sequence, type, key, value);
    } catch (const exception& exception) {
        status = Status::IOError(exception.what());
    }
    lock.lock();
    if (status.ok()) lastSequence_ = sequence;
    else if (backgroundError_.ok()) backgroundError_ = status;
    --activeOperations_;
    operationsCV_.notify_all();
    spaceCV_.notify_all();
    return status;
}

Status DB::MakeRoomForWrite(unique_lock<mutex>& lock) {
    for (;;) {
        if (shuttingDown_) return Status::Closed();
        if (!backgroundError_.ok()) return backgroundError_;
        if (mem_->MemoryUsage() < kMemtableLimit) return {};
        if (imm_) {
            spaceCV_.wait(lock);
            continue;
        }
        const uint64_t number = versions_.NewFileNumber();
        if (number == 0) return Status::IOError("file numbers exhausted");
        lock.unlock();
        MemTable* nextMem = nullptr;
        auto nextWal = make_unique<WAL>();
        Status status;
        try {
            nextMem = new MemTable;
            status = nextWal->Create(path_ + "/" + FileName(number, ".wal"));
        } catch (...) {
            if (nextMem) nextMem->Unref();
            throw;
        }
        lock.lock();
        if (!status.ok() || shuttingDown_ || !backgroundError_.ok()) {
            nextMem->Unref();
            if (!status.ok()) return status;
            return shuttingDown_ ? Status::Closed() : backgroundError_;
        }
        imm_ = mem_;
        immWal_ = std::move(wal_);
        mem_ = nextMem;
        wal_ = std::move(nextWal);
        currentWalId_ = number;
        memOldestWal_ = number;
        ScheduleFlush();
    }
}

Status DB::Get(string_view key, string& value) {
    MemTable* mem;
    MemTable* imm;
    uint64_t snapshotSeq;
    shared_ptr<const Version> version;
    {
        lock_guard<mutex> lock(dbMutex_);
        if (!opened_ || shuttingDown_) return Status::Closed();
        ++activeOperations_;
        mem = mem_;
        imm = imm_;
        mem->Ref();
        if (imm) imm->Ref();
        snapshotSeq = lastSequence_;
        version = versions_.Current();
    }
    GetResult result;
    try {
        result = mem->Get(key, snapshotSeq);
        if (result.state == GetState::Absent && imm) result = imm->Get(key, snapshotSeq);
        if (result.state == GetState::Absent) result = versions_.Get(key, snapshotSeq, version);
        if (result.state == GetState::Found) value = std::move(result.value);
        else value.clear();
    } catch (const exception& exception) {
        result = {GetState::Error, {}, 0, Status::IOError(exception.what())};
    }
    mem->Unref();
    if (imm) imm->Unref();
    version.reset();
    {
        lock_guard<mutex> lock(dbMutex_);
        --activeOperations_;
        operationsCV_.notify_all();
    }
    if (result.state == GetState::Error) return result.status;
    return result.state == GetState::Found ? Status{} : Status::NotFound();
}

Status DB::BuildTable(MemTable* mem, uint64_t number, FileMetadata& metadata) {
    SSTableBuilder builder(path_ + "/" + FileName(number, ".sst"), number);
    auto iterator = mem->NewIterator();
    for (iterator.SeekToFirst(); iterator.Valid(); iterator.Next()) {
        Status status = builder.Add(iterator.key(), iterator.value());
        if (!status.ok()) return status;
    }
    return builder.Finish(metadata);
}

Status DB::FlushRecoveredMemTable() {
    const uint64_t number = versions_.NewFileNumber();
    if (number == 0) return Status::IOError("file numbers exhausted");
    FileMetadata metadata;
    Status status = BuildTable(mem_, number, metadata);
    if (!status.ok()) return status;
    VersionEdit edit;
    edit.lastSequence = lastSequence_;
    edit.oldestRequiredWal = oldestRequiredWal_;
    edit.nextFileNumber = versions_.NextFileNumber();
    edit.added.push_back(std::move(metadata));
    shared_ptr<Version> next;
    status = versions_.BuildNext(edit, next);
    if (status.ok()) status = manifest_.AppendAndSync(edit);
    if (!status.ok()) return status;
    versions_.Publish(std::move(next));
    MemTable* previous = mem_;
    mem_ = new MemTable;
    previous->Unref();
    return {};
}

Status DB::FlushMemTable() {
    MemTable* mem;
    uint64_t number, oldest;
    {
        lock_guard<mutex> lock(dbMutex_);
        mem = imm_;
        mem->Ref();
        number = versions_.NewFileNumber();
        oldest = memOldestWal_;
    }
    if (number == 0) {
        mem->Unref();
        return Status::IOError("file numbers exhausted");
    }
    Status status;
    try {
        FileMetadata metadata;
        status = BuildTable(mem, number, metadata);
        VersionEdit edit;
        shared_ptr<Version> next;
        if (status.ok()) {
            edit.added.push_back(std::move(metadata));
            lock_guard<mutex> lock(dbMutex_);
            edit.lastSequence = lastSequence_;
            edit.oldestRequiredWal = oldest;
            edit.nextFileNumber = versions_.NextFileNumber();
            status = versions_.BuildNext(edit, next);
        }
        if (status.ok()) status = manifest_.AppendAndSync(edit);
        if (status.ok()) {
            unique_ptr<WAL> obsoleteWal;
            {
                lock_guard<mutex> lock(dbMutex_);
                versions_.Publish(std::move(next));
                imm_ = nullptr;
                oldestRequiredWal_ = oldest;
                obsoleteWal = std::move(immWal_);
            }
            mem->Unref();
            status = obsoleteWal->Close();
            if (status.ok()) status = DeleteObsoleteWALs(oldest);
            spaceCV_.notify_all();
        }
    } catch (...) {
        mem->Unref();
        throw;
    }
    mem->Unref();
    return status;
}

void DB::FlushWorker() {
    unique_lock<mutex> lock(dbMutex_);
    for (;;) {
        flushCV_.wait(lock, [&] {
            return shuttingDown_ || (backgroundError_.ok() && (imm_ || versions_.NeedsCompaction()));
        });
        if (shuttingDown_) return;
        const bool flush = imm_ != nullptr;
        lock.unlock();
        Status status;
        try {
            status = flush ? FlushMemTable() : CompactLevel();
        } catch (const exception& exception) {
            status = Status::IOError(exception.what());
        }
        lock.lock();
        if (!status.ok() && backgroundError_.ok()) backgroundError_ = status;
        spaceCV_.notify_all();
    }
}

Status DB::DeleteObsoleteWALs(uint64_t oldest) {
    error_code error;
    bool removed = false;
    for (filesystem::directory_iterator it(path_, error), end; !error && it != end; it.increment(error)) {
        uint64_t number;
        if (ParseFileName(it->path().filename().string(), ".wal", number) && number < oldest) {
            filesystem::remove(it->path(), error);
            if (error) break;
            removed = true;
        }
    }
    if (error) return Status::IOError("remove obsolete WAL: " + error.message());
    return removed ? SyncDirectory(path_) : Status{};
}

Status DB::CompactLevel() {
    shared_ptr<const Version> version;
    vector<shared_ptr<TableFile>> inputs;
    int level;
    {
        lock_guard<mutex> lock(dbMutex_);
        level = versions_.CompactionLevel();
        if (level < 0) return {};
        version = versions_.Current();
    }
    string smallest, largest;
    for (const auto& file : version->files) {
        if (file->metadata.level != static_cast<uint32_t>(level)) continue;
        inputs.push_back(file);
        const auto low = UserKey(file->metadata.smallest);
        const auto high = UserKey(file->metadata.largest);
        if (inputs.size() == 1 || low < smallest) smallest.assign(low);
        if (inputs.size() == 1 || high > largest) largest.assign(high);
        if (level != 0) break;
    }
    for (const auto& file : version->files) {
        if (file->metadata.level == static_cast<uint32_t>(level + 1) &&
            UserKey(file->metadata.smallest) <= largest && UserKey(file->metadata.largest) >= smallest)
            inputs.push_back(file);
    }
    vector<unique_ptr<SSTableReader>> readers;
    vector<unique_ptr<SSTableReader::Iterator>> iterators;
    for (const auto& input : inputs) {
        auto reader = make_unique<SSTableReader>();
        Status status = reader->Open(input->path);
        if (!status.ok()) return status;
        auto iterator = reader->NewIterator();
        iterator->SeekToFirst();
        if (!iterator->status().ok()) return iterator->status();
        readers.push_back(std::move(reader));
        iterators.push_back(std::move(iterator));
    }
    const auto later = [&](size_t a, size_t b) {
        return CompareInternalKeys(iterators[a]->key(), iterators[b]->key()) > 0;
    };
    priority_queue<size_t, vector<size_t>, decltype(later)> heap(later);
    for (size_t i = 0; i < iterators.size(); ++i)
        if (iterators[i]->Valid()) heap.push(i);
    VersionEdit edit;
    for (const auto& input : inputs)
        edit.removed.emplace_back(input->metadata.level, input->metadata.number);
    unique_ptr<SSTableBuilder> builder;
    string lastKey;
    while (!heap.empty()) {
        const size_t index = heap.top();
        heap.pop();
        auto& iterator = iterators[index];
        if (lastKey.empty() || CompareInternalKeys(lastKey, iterator->key()) != 0) {
            if (builder && builder->FileSize() >= 2 * 1024 * 1024 &&
                UserKey(lastKey) != UserKey(iterator->key())) {
                FileMetadata metadata;
                Status status = builder->Finish(metadata);
                if (!status.ok()) return status;
                edit.added.push_back(std::move(metadata));
                builder.reset();
            }
            if (!builder) {
                uint64_t number;
                {
                    lock_guard<mutex> lock(dbMutex_);
                    number = versions_.NewFileNumber();
                }
                if (number == 0) return Status::IOError("file numbers exhausted");
                builder = make_unique<SSTableBuilder>(
                    path_ + "/" + FileName(number, ".sst"), number, level + 1);
            }
            Status status = builder->Add(iterator->key(), iterator->value());
            if (!status.ok()) return status;
            lastKey.assign(iterator->key());
        }
        iterator->Next();
        if (!iterator->status().ok()) return iterator->status();
        if (iterator->Valid()) heap.push(index);
    }
    if (builder) {
        FileMetadata metadata;
        Status status = builder->Finish(metadata);
        if (!status.ok()) return status;
        edit.added.push_back(std::move(metadata));
    }
    shared_ptr<Version> next;
    Status status;
    {
        lock_guard<mutex> lock(dbMutex_);
        edit.lastSequence = lastSequence_;
        edit.oldestRequiredWal = oldestRequiredWal_;
        edit.nextFileNumber = versions_.NextFileNumber();
        status = versions_.BuildNext(edit, next);
    }
    if (status.ok()) status = manifest_.AppendAndSync(edit);
    if (!status.ok()) return status;
    {
        lock_guard<mutex> lock(dbMutex_);
        versions_.Publish(std::move(next));
    }
    return {};
}

Status DB::Close() {
    lock_guard<mutex> lifecycle(lifecycleMutex_);
    {
        unique_lock<mutex> lock(dbMutex_);
        if (!opened_) return closeStatus_;
        shuttingDown_ = true;
        flushCV_.notify_all();
        spaceCV_.notify_all();
        operationsCV_.wait(lock, [&] { return activeOperations_ == 0; });
    }
    if (flushThread_.joinable()) flushThread_.join();
    closeStatus_ = backgroundError_;
    for (WAL* log : {wal_.get(), immWal_.get()}) {
        if (!log) continue;
        Status status = log->Sync();
        if (!status.ok() && closeStatus_.ok()) closeStatus_ = status;
        status = log->Close();
        if (!status.ok() && closeStatus_.ok()) closeStatus_ = status;
    }
    Status status = manifest_.Close();
    if (!status.ok() && closeStatus_.ok()) closeStatus_ = status;
    {
        lock_guard<mutex> lock(dbMutex_);
        opened_ = false;
        ReleaseResources();
    }
    return closeStatus_;
}

void DB::ReleaseResources() {
    wal_.reset();
    immWal_.reset();
    if (mem_) mem_->Unref();
    if (imm_) imm_->Unref();
    mem_ = nullptr;
    imm_ = nullptr;
    versions_.Clear();
    manifest_.Close();
    if (lockFd_ >= 0) {
        ::flock(lockFd_, LOCK_UN);
        ::close(lockFd_);
        lockFd_ = -1;
    }
}

}
