#pragma once

#include "manifest.h"
#include "memtable.h"
#include "version_set.h"
#include "wal.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace quorum {

class DB {
public:
    explicit DB(std::string path) : path_(std::move(path)), versions_(path_) {}
    ~DB();
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
    Status Open();
    Status Put(std::string_view key, std::string_view value, bool sync = false);
    Status Delete(std::string_view key, bool sync = false);
    Status Get(std::string_view key, std::string& value);
    Status Close();

private:
    static constexpr std::size_t kMemtableLimit = 4 * 1024 * 1024;
    Status Write(ValueType type, std::string_view key, std::string_view value, bool sync);
    Status MakeRoomForWrite(std::unique_lock<std::mutex>& lock);
    void ScheduleFlush() { flushCV_.notify_one(); }
    void FlushWorker();
    Status FlushMemTable();
    Status BuildTable(MemTable* mem, uint64_t number, FileMetadata& metadata);
    Status FlushRecoveredMemTable();
    Status CompactLevel();
    Status Recover();
    Status DeleteObsoleteWALs(uint64_t oldest);
    void ReleaseResources();

    std::string path_;
    MemTable* mem_ = nullptr;
    MemTable* imm_ = nullptr;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<WAL> immWal_;
    std::mutex writerMutex_;
    std::mutex dbMutex_;
    std::mutex lifecycleMutex_;
    std::condition_variable flushCV_;
    std::condition_variable spaceCV_;
    std::condition_variable operationsCV_;
    std::thread flushThread_;
    uint64_t lastSequence_ = 0;
    uint64_t currentWalId_ = 0;
    uint64_t oldestRequiredWal_ = 1;
    uint64_t memOldestWal_ = 1;
    std::size_t activeOperations_ = 0;
    bool shuttingDown_ = false;
    bool opened_ = false;
    int lockFd_ = -1;
    Status backgroundError_;
    Status closeStatus_;
    VersionSet versions_;
    Manifest manifest_;
};

}
