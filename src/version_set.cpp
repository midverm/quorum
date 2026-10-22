#include "version_set.h"

#include "wal.h"

#include <algorithm>
#include <array>
#include <limits>
#include <unordered_set>
#include <unistd.h>

namespace quorum {

using namespace std;

TableFile::~TableFile() {
    if (!fileOwner && obsolete.load(memory_order_relaxed)) ::unlink(path.c_str());
}

uint64_t VersionSet::NewFileNumber() {
    if (nextFileNumber_ == numeric_limits<uint64_t>::max()) return 0;
    return nextFileNumber_++;
}

Status VersionSet::BuildNext(const VersionEdit& edit, shared_ptr<Version>& next) const {
    auto result = make_shared<Version>();
    unordered_set<uint64_t> removed;
    for (const auto& removal : edit.removed) {
        if (!removed.insert(removal.second).second)
            return Status::Corruption("duplicate table removal");
        const auto found = find_if(current_->files.begin(), current_->files.end(),
            [&](const auto& file) {
                return file->metadata.number == removal.second && file->metadata.level == removal.first;
            });
        if (found == current_->files.end()) return Status::Corruption("removed table is missing");
    }
    unordered_set<uint64_t> numbers;
    for (const auto& file : current_->files) {
        if (removed.count(file->metadata.number) == 0) {
            numbers.insert(file->metadata.number);
            result->files.push_back(file);
        }
    }
    for (const auto& metadata : edit.added) {
        if (metadata.number == 0 || metadata.number >= edit.nextFileNumber || metadata.level > 31 ||
            metadata.fileSize < 24 || !ValidInternalKey(metadata.smallest) ||
            !ValidInternalKey(metadata.largest) || Sequence(metadata.smallest) == 0 ||
            Sequence(metadata.largest) == 0 ||
            CompareInternalKeys(metadata.smallest, metadata.largest) > 0 ||
            !numbers.insert(metadata.number).second)
            return Status::Corruption("invalid or duplicate table metadata");
        auto file = make_shared<TableFile>();
        file->metadata = metadata;
        file->path = path_ + "/" + FileName(metadata.number, ".sst");
        const auto existing = find_if(current_->files.begin(), current_->files.end(),
            [&](const auto& input) { return input->metadata.number == metadata.number; });
        if (existing != current_->files.end()) {
            const auto& old = (*existing)->metadata;
            if (!removed.count(metadata.number) || metadata.level != old.level + 1 ||
                metadata.fileSize != old.fileSize || metadata.smallest != old.smallest ||
                metadata.largest != old.largest)
                return Status::Corruption("invalid SSTable move");
            file->fileOwner = (*existing)->fileOwner ? (*existing)->fileOwner : *existing;
        }
        result->files.push_back(std::move(file));
    }
    sort(result->files.begin(), result->files.end(), [](const auto& a, const auto& b) {
        if (a->metadata.level != b->metadata.level) return a->metadata.level < b->metadata.level;
        if (a->metadata.level == 0) return a->metadata.number > b->metadata.number;
        return CompareInternalKeys(a->metadata.smallest, b->metadata.smallest) < 0;
    });
    for (size_t i = 1; i < result->files.size(); ++i) {
        const auto& a = result->files[i - 1]->metadata;
        const auto& b = result->files[i]->metadata;
        if (a.level != 0 && a.level == b.level && UserKey(a.largest) >= UserKey(b.smallest))
            return Status::Corruption("overlapping tables above level zero");
    }
    next = std::move(result);
    return {};
}

void VersionSet::Publish(shared_ptr<Version> next) {
    auto previous = std::move(current_);
    current_ = std::move(next);
    for (const auto& file : previous->files) {
        const bool retained = any_of(current_->files.begin(), current_->files.end(),
            [&](const auto& candidate) { return candidate->metadata.number == file->metadata.number; });
        if (!retained) {
            const auto& owner = file->fileOwner ? file->fileOwner : file;
            owner->obsolete.store(true, memory_order_relaxed);
        }
    }
}

GetResult VersionSet::Get(string_view key, uint64_t snapshotSeq,
                          const shared_ptr<const Version>& version) const {
    GetResult result;
    for (const auto& file : version->files) {
        const auto& metadata = file->metadata;
        if (key < UserKey(metadata.smallest) || key > UserKey(metadata.largest)) continue;
        SSTableReader reader;
        Status status = reader.Open(file->path);
        if (!status.ok()) return {GetState::Error, {}, 0, status};
        auto candidate = reader.Get(key, snapshotSeq);
        if (candidate.state == GetState::Error) return candidate;
        if (candidate.state != GetState::NotFound &&
            (result.state == GetState::NotFound || candidate.sequence > result.sequence))
            result = std::move(candidate);
    }
    return result;
}

int VersionSet::CompactionLevel() const {
    array<uint64_t, 32> sizes{};
    size_t levelZero = 0;
    for (const auto& file : current_->files) {
        if (file->metadata.level == 0) ++levelZero;
        const auto level = file->metadata.level;
        if (sizes[level] > numeric_limits<uint64_t>::max() - file->metadata.fileSize)
            sizes[level] = numeric_limits<uint64_t>::max();
        else sizes[level] += file->metadata.fileSize;
    }
    double bestScore = static_cast<double>(levelZero) / 4;
    int selected = bestScore >= 1 ? 0 : -1;
    uint64_t limit = 10 * 1024 * 1024;
    for (int level = 1; level < 31; ++level) {
        const double score = static_cast<double>(sizes[level]) / limit;
        if (score >= 1 && (selected < 0 || score > bestScore)) {
            bestScore = score;
            selected = level;
        }
        if (limit <= numeric_limits<uint64_t>::max() / 10) limit *= 10;
        else limit = numeric_limits<uint64_t>::max();
    }
    return selected;
}

CompactionPlan VersionSet::PickCompaction() const {
    CompactionPlan plan;
    plan.sourceLevel = CompactionLevel();
    if (plan.sourceLevel < 0) return plan;
    plan.destinationLevel = plan.sourceLevel + 1;
    string smallest, largest;
    for (const auto& file : current_->files) {
        if (file->metadata.level != static_cast<uint32_t>(plan.sourceLevel)) continue;
        plan.inputs.push_back(file);
        const auto low = UserKey(file->metadata.smallest);
        const auto high = UserKey(file->metadata.largest);
        if (plan.inputs.size() == 1 || low < smallest) smallest.assign(low);
        if (plan.inputs.size() == 1 || high > largest) largest.assign(high);
        if (plan.sourceLevel != 0) break;
    }
    for (const auto& file : current_->files) {
        if (file->metadata.level == static_cast<uint32_t>(plan.destinationLevel) &&
            UserKey(file->metadata.smallest) <= largest && UserKey(file->metadata.largest) >= smallest)
            plan.inputs.push_back(file);
    }
    return plan;
}

}
