#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace quorum {

class BloomFilter {
public:
    void Add(std::string_view key);
    std::string Finish() const;
    static bool MayContain(std::string_view key, std::string_view filter);

private:
    static uint64_t Hash(std::string_view key);
    std::vector<uint64_t> hashes_;
};

}
