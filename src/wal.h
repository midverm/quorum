#pragma once

#include "memtable.h"
#include "status.h"

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace quorum {

constexpr uint32_t kMaxRecordSize = 64 * 1024 * 1024;

void Put32(std::string& output, uint32_t value);
void Put64(std::string& output, uint64_t value);
bool Take32(std::string_view& input, uint32_t& value);
bool Take64(std::string_view& input, uint64_t& value);
void PutString(std::string& output, std::string_view value);
bool TakeString(std::string_view& input, std::string& value);
uint32_t Checksum(std::string_view bytes);
Status FileError(std::string_view operation);
Status WriteAll(int fd, std::string_view bytes);
Status ReadAt(int fd, uint64_t offset, std::size_t size, std::string& bytes);
Status SyncFile(int fd);
Status SyncDirectory(const std::string& path);
Status AppendRecord(int fd, std::string_view payload);
Status ReadRecord(int fd, uint64_t fileSize, uint64_t& offset, std::string& payload,
                  bool& complete);
std::string FileName(uint64_t number, std::string_view extension);
bool ParseFileName(std::string_view name, std::string_view extension, uint64_t& number);

class WAL {
public:
    WAL() = default;
    ~WAL();
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    Status Create(const std::string& path);
    Status Append(uint64_t sequence, ValueType type, std::string_view key,
                  std::string_view value);
    Status Sync();
    Status Close();
    static Status Replay(const std::string& path, MemTable*& mem, uint64_t& maxSeq,
                         const std::function<Status()>& afterRecord = {});

private:
    int fd_ = -1;
};

}
