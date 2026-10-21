#pragma once

#include <string>
#include <utility>

namespace quorum {

class Status {
public:
    enum class Code { Ok, NotFound, InvalidArgument, IOError, Corruption, Closed };

    Status() = default;
    Status(Code code, std::string message) : code_(code), message_(std::move(message)) {}

    static Status OK() { return {}; }
    static Status NotFound() { return {Code::NotFound, "key not found"}; }
    static Status InvalidArgument(std::string message) {
        return {Code::InvalidArgument, std::move(message)};
    }
    static Status IOError(std::string message) { return {Code::IOError, std::move(message)}; }
    static Status Corruption(std::string message) {
        return {Code::Corruption, std::move(message)};
    }
    static Status Closed() { return {Code::Closed, "database is closed"}; }

    bool ok() const { return code_ == Code::Ok; }
    bool IsNotFound() const { return code_ == Code::NotFound; }
    Code code() const { return code_; }
    const std::string& message() const { return message_; }

private:
    Code code_ = Code::Ok;
    std::string message_;
};

}
