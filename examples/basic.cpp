#include "db.h"

#include <iostream>
#include <stdexcept>
#include <string>

void Check(const quorum::Status& status) {
    if (!status.ok()) throw std::runtime_error(status.message());
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <database-directory>\n";
        return 1;
    }
    try {
        quorum::DB db(argv[1]);
        Check(db.Open());
        Check(db.Put("example:greeting", "Hello, Quorum!", true));
        Check(db.Close());

        // Reopening recovers the value saved in the same directory.
        Check(db.Open());
        std::string value;
        Check(db.Get("example:greeting", value));
        if (value != "Hello, Quorum!") throw std::runtime_error("recovered value mismatch");
        std::cout << "example:greeting = " << value << '\n';

        // To remove a key: Check(db.Delete("example:greeting", true));
        // For optional keys, handle Get(...).IsNotFound() before treating it as an error.
        Check(db.Close());
    } catch (const std::exception& error) {
        std::cerr << "Database error: " << error.what() << '\n';
        return 1;
    }
}
