#include "wal.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <limits>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace quorum {

using namespace std;

void Put32(string& output, uint32_t value) {
    for (int i = 0; i < 4; ++i) output.push_back(static_cast<char>(value >> (i * 8)));
}

void Put64(string& output, uint64_t value) {
    for (int i = 0; i < 8; ++i) output.push_back(static_cast<char>(value >> (i * 8)));
}

bool Take32(string_view& input, uint32_t& value) {
    if (input.size() < 4) return false;
    value = 0;
    for (int i = 0; i < 4; ++i)
        value |= static_cast<uint32_t>(static_cast<uint8_t>(input[i])) << (i * 8);
    input.remove_prefix(4);
    return true;
}

bool Take64(string_view& input, uint64_t& value) {
    if (input.size() < 8) return false;
    value = 0;
    for (int i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(static_cast<uint8_t>(input[i])) << (i * 8);
    input.remove_prefix(8);
    return true;
}

void PutString(string& output, string_view value) {
    Put32(output, static_cast<uint32_t>(value.size()));
    output.append(value);
}

bool TakeString(string_view& input, string& value) {
    uint32_t size;
    if (!Take32(input, size) || size > input.size()) return false;
    value.assign(input.data(), size);
    input.remove_prefix(size);
    return true;
}

uint32_t Checksum(string_view bytes) {
    static const auto table = [] {
        array<uint32_t, 256> result{};
        for (uint32_t i = 0; i < result.size(); ++i) {
            uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit)
                value = (value >> 1) ^ ((value & 1) ? 0xedb88320U : 0);
            result[i] = value;
        }
        return result;
    }();
    uint32_t crc = 0xffffffffU;
    for (unsigned char byte : bytes) crc = table[(crc ^ byte) & 255] ^ (crc >> 8);
    return ~crc;
}

Status FileError(string_view operation) {
    return Status::IOError(string(operation) + ": " + strerror(errno));
}

Status WriteAll(int fd, string_view bytes) {
    while (!bytes.empty()) {
        const ssize_t count = ::write(fd, bytes.data(), bytes.size());
        if (count < 0) {
            if (errno == EINTR) continue;
            return FileError("write");
        }
        if (count == 0) return Status::IOError("write made no progress");
        bytes.remove_prefix(static_cast<size_t>(count));
    }
    return {};
}

Status ReadAt(int fd, uint64_t offset, size_t size, string& bytes) {
    if (offset > static_cast<uint64_t>(numeric_limits<off_t>::max()) ||
        size > static_cast<uint64_t>(numeric_limits<off_t>::max()) - offset)
        return Status::Corruption("file offset overflow");
    bytes.resize(size);
    size_t done = 0;
    while (done < size) {
        const ssize_t count = ::pread(fd, bytes.data() + done, size - done,
                                       static_cast<off_t>(offset + done));
        if (count < 0) {
            if (errno == EINTR) continue;
            return FileError("pread");
        }
        if (count == 0) return Status::Corruption("unexpected end of file");
        done += static_cast<size_t>(count);
    }
    return {};
}

Status SyncFile(int fd) {
    while (::fsync(fd) != 0) {
        if (errno != EINTR) return FileError("fsync");
    }
    return {};
}

Status SyncDirectory(const string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return FileError("open directory " + path);
    Status status = SyncFile(fd);
    if (::close(fd) != 0 && status.ok()) status = FileError("close directory");
    return status;
}

Status AppendRecord(int fd, string_view payload) {
    if (payload.size() > kMaxRecordSize) return Status::InvalidArgument("record exceeds 64 MiB");
    string header;
    Put32(header, static_cast<uint32_t>(payload.size()));
    Put32(header, Checksum(payload));
    Status status = WriteAll(fd, header);
    if (status.ok()) status = WriteAll(fd, payload);
    return status;
}

Status ReadRecord(int fd, uint64_t fileSize, uint64_t& offset, string& payload,
                  bool& complete) {
    complete = false;
    if (offset > fileSize) return Status::Corruption("record offset beyond file");
    if (fileSize - offset < 8) return {};
    string header;
    Status status = ReadAt(fd, offset, 8, header);
    if (!status.ok()) return status;
    string_view view(header);
    uint32_t size, checksum;
    if (!Take32(view, size) || !Take32(view, checksum))
        return Status::Corruption("short record header");
    if (size > kMaxRecordSize) return Status::Corruption("invalid record length");
    if (size > fileSize - offset - 8) return {};
    status = ReadAt(fd, offset + 8, size, payload);
    if (!status.ok()) return status;
    if (Checksum(payload) != checksum) return Status::Corruption("record checksum mismatch");
    offset += 8 + size;
    complete = true;
    return {};
}

string FileName(uint64_t number, string_view extension) {
    ostringstream output;
    output << setw(20) << setfill('0') << number << extension;
    return output.str();
}

bool ParseFileName(string_view name, string_view extension, uint64_t& number) {
    if (name.size() <= extension.size() || name.substr(name.size() - extension.size()) != extension)
        return false;
    name.remove_suffix(extension.size());
    number = 0;
    for (char digit : name) {
        if (digit < '0' || digit > '9' ||
            number > (numeric_limits<uint64_t>::max() - (digit - '0')) / 10)
            return false;
        number = number * 10 + static_cast<unsigned>(digit - '0');
    }
    return number != 0;
}

WAL::~WAL() { Close(); }

Status WAL::Create(const string& path) {
    if (fd_ >= 0) return Status::InvalidArgument("WAL already open");
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd_ < 0) return FileError("create WAL " + path);
    Status status = SyncDirectory(filesystem::path(path).parent_path().string());
    if (!status.ok()) Close();
    return status;
}

Status WAL::Append(uint64_t sequence, ValueType type, string_view key,
                   string_view value) {
    if (fd_ < 0) return Status::Closed();
    if (sequence == 0 || static_cast<uint8_t>(type) > 1 ||
        (type == ValueType::Deletion && !value.empty()) ||
        key.size() > kMaxRecordSize - 17 || value.size() > kMaxRecordSize - 17 - key.size())
        return Status::InvalidArgument("invalid WAL entry");
    string payload;
    Put64(payload, sequence);
    payload.push_back(static_cast<char>(type));
    PutString(payload, key);
    PutString(payload, value);
    return AppendRecord(fd_, payload);
}

Status WAL::Sync() { return fd_ < 0 ? Status::Closed() : SyncFile(fd_); }

Status WAL::Close() {
    if (fd_ < 0) return {};
    const int fd = fd_;
    fd_ = -1;
    return ::close(fd) == 0 ? Status{} : FileError("close WAL");
}

Status WAL::Replay(const string& path, MemTable*& mem, uint64_t& maxSeq,
                   const function<Status()>& afterRecord) {
    WAL replay;
    const int fd = replay.fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return FileError("open WAL " + path);
    struct stat info{};
    Status status;
    if (::fstat(fd, &info) != 0) status = FileError("stat WAL");
    uint64_t offset = 0, previousSequence = 0;
    while (status.ok()) {
        string payload;
        bool complete;
        status = ReadRecord(fd, static_cast<uint64_t>(info.st_size), offset, payload, complete);
        if (!status.ok() || !complete) break;
        string_view view(payload);
        uint64_t sequence;
        string key, value;
        if (!Take64(view, sequence) || view.empty()) {
            status = Status::Corruption("invalid WAL entry");
            break;
        }
        const auto type = static_cast<ValueType>(view.front());
        view.remove_prefix(1);
        if (sequence <= previousSequence || static_cast<uint8_t>(type) > 1 ||
            !TakeString(view, key) || !TakeString(view, value) || !view.empty() ||
            (type == ValueType::Deletion && !value.empty())) {
            status = Status::Corruption("invalid WAL entry");
            break;
        }
        mem->Add(sequence, type, key, value);
        previousSequence = sequence;
        maxSeq = max(maxSeq, sequence);
        if (afterRecord) status = afterRecord();
    }
    Status closeStatus = replay.Close();
    if (status.ok()) status = closeStatus;
    return status;
}

}
