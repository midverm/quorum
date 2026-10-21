#include "manifest.h"

#include "wal.h"

#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace quorum {

using namespace std;

Status Manifest::DecodeEdit(string_view bytes, VersionEdit& edit) {
    uint32_t format = 0, added = 0, removed = 0;
    if (!Take32(bytes, format) || format != kManifestFormat ||
        !Take64(bytes, edit.lastSequence) || !Take64(bytes, edit.oldestRequiredWal) ||
        !Take64(bytes, edit.nextFileNumber) || edit.oldestRequiredWal == 0 ||
        edit.nextFileNumber == 0 || !Take32(bytes, added) || added > bytes.size() / 46)
        return Status::Corruption("invalid MANIFEST edit");
    for (uint32_t i = 0; i < added; ++i) {
        FileMetadata file;
        if (!Take64(bytes, file.number) || !Take32(bytes, file.level) ||
            !Take64(bytes, file.fileSize) || !TakeString(bytes, file.smallest) ||
            !TakeString(bytes, file.largest) || !ValidInternalKey(file.smallest) ||
            !ValidInternalKey(file.largest) || file.number == 0 || file.level > 31 ||
            file.fileSize < 24 || Sequence(file.smallest) == 0 || Sequence(file.largest) == 0 ||
            CompareInternalKeys(file.smallest, file.largest) > 0)
            return Status::Corruption("invalid MANIFEST table metadata");
        edit.added.push_back(std::move(file));
    }
    if (!Take32(bytes, removed) || removed > bytes.size() / 12)
        return Status::Corruption("invalid MANIFEST removal count");
    for (uint32_t i = 0; i < removed; ++i) {
        uint32_t level;
        uint64_t number;
        if (!Take32(bytes, level) || !Take64(bytes, number) || level > 31 || number == 0)
            return Status::Corruption("invalid MANIFEST removal");
        edit.removed.emplace_back(level, number);
    }
    if (!bytes.empty()) return Status::Corruption("trailing MANIFEST bytes");
    return {};
}

Manifest::~Manifest() { Close(); }

Status Manifest::Create(const string& path) {
    if (fd_ >= 0) return Status::InvalidArgument("MANIFEST already open");
    fd_ = ::open(path.c_str(), O_RDWR | O_APPEND | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd_ < 0) return FileError("create MANIFEST " + path);
    error_ = SyncFile(fd_);
    if (error_.ok()) error_ = SyncDirectory(filesystem::path(path).parent_path().string());
    return error_;
}

Status Manifest::Recover(const string& path, vector<VersionEdit>& edits) {
    if (fd_ >= 0) return Status::InvalidArgument("MANIFEST already open");
    error_ = {};
    fd_ = ::open(path.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
    if (fd_ < 0) return FileError("open MANIFEST " + path);
    edits.clear();
    struct stat info{};
    if (::fstat(fd_, &info) != 0) return error_ = FileError("stat MANIFEST");
    uint64_t offset = 0;
    while (offset < static_cast<uint64_t>(info.st_size)) {
        string payload;
        bool complete;
        error_ = ReadRecord(fd_, info.st_size, offset, payload, complete);
        if (!error_.ok()) return error_;
        if (!complete) break;
        VersionEdit edit;
        error_ = DecodeEdit(payload, edit);
        if (!error_.ok()) return error_;
        if (!edits.empty() && (edit.lastSequence < edits.back().lastSequence ||
            edit.oldestRequiredWal < edits.back().oldestRequiredWal ||
            edit.nextFileNumber < edits.back().nextFileNumber))
            return error_ = Status::Corruption("MANIFEST counters moved backwards");
        edits.push_back(std::move(edit));
    }
    if (offset != static_cast<uint64_t>(info.st_size)) {
        if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0)
            return error_ = FileError("truncate incomplete MANIFEST tail");
        error_ = SyncFile(fd_);
    }
    return error_;
}

Status Manifest::AppendAndSync(const VersionEdit& edit) {
    if (fd_ < 0) return Status::Closed();
    if (!error_.ok()) return error_;
    string bytes;
    Put32(bytes, kManifestFormat);
    Put64(bytes, edit.lastSequence);
    Put64(bytes, edit.oldestRequiredWal);
    Put64(bytes, edit.nextFileNumber);
    Put32(bytes, static_cast<uint32_t>(edit.added.size()));
    for (const auto& file : edit.added) {
        if (file.smallest.size() > kMaxRecordSize || file.largest.size() > kMaxRecordSize ||
            bytes.size() + file.smallest.size() + file.largest.size() + 28 > kMaxRecordSize)
            return Status::InvalidArgument("MANIFEST edit exceeds 64 MiB");
        Put64(bytes, file.number);
        Put32(bytes, file.level);
        Put64(bytes, file.fileSize);
        PutString(bytes, file.smallest);
        PutString(bytes, file.largest);
    }
    Put32(bytes, static_cast<uint32_t>(edit.removed.size()));
    for (const auto& file : edit.removed) {
        if (bytes.size() + 12 > kMaxRecordSize)
            return Status::InvalidArgument("MANIFEST edit exceeds 64 MiB");
        Put32(bytes, file.first);
        Put64(bytes, file.second);
    }
    error_ = AppendRecord(fd_, bytes);
    if (error_.ok()) error_ = SyncFile(fd_);
    return error_;
}

Status Manifest::Close() {
    if (fd_ < 0) return {};
    const int fd = fd_;
    fd_ = -1;
    return ::close(fd) == 0 ? Status{} : FileError("close MANIFEST");
}

}
