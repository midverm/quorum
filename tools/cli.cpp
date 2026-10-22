#include "db.h"

#include <iostream>
#include <string>
#include <string_view>

int main(int argc, char** argv) {
    auto usage = [&] {
        std::cerr << "usage: " << argv[0] << " <folder> put <key> <value>\n"
                  << "       " << argv[0] << " <folder> get <key>\n"
                  << "       " << argv[0] << " <folder> delete <key>\n";
    };
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        usage();
        return 0;
    }
    const std::string_view command = argc > 2 ? argv[2] : "";
    if (!((command == "put" && argc == 5) ||
          ((command == "get" || command == "delete") && argc == 4))) {
        usage();
        return 1;
    }

    quorum::DB db(argv[1]);
    auto status = db.Open();
    if (!status.ok()) {
        std::cerr << "error: " << status.message() << '\n';
        return 1;
    }

    std::string value;
    if (command == "put") status = db.Put(argv[3], argv[4], true);
    else if (command == "delete") status = db.Delete(argv[3], true);
    else status = db.Get(argv[3], value);

    const auto closed = db.Close();
    if (!status.ok() || !closed.ok()) {
        std::cerr << "error: " << (!status.ok() ? status.message() : closed.message()) << '\n';
        return 1;
    }
    if (command == "get") std::cout << value << '\n';
    else std::cout << "ok\n";
}
