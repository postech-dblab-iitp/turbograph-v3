#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <stdexcept>
#include <cctype>

#include "storage/cache/log_disk.h"
#include "storage/cache/object_log.h"
#include "storage/cache/store.h"

long parseSize(const std::string& sizeStr) {
    std::istringstream iss(sizeStr);
    long size;
    std::string unit;
    iss >> size >> unit;

    if (unit == "B" || unit == "b") {
        return size;
    } else if (unit == "KB" || unit == "kb" || unit == "Kb") {
        return size * 1024L;
    } else if (unit == "MB" || unit == "mb" || unit == "Mb") {
        return size * 1024L * 1024L;
    } else if (unit == "GB" || unit == "gb" || unit == "Gb") {
        return size * 1024L * 1024L * 1024L;
    } else if (unit == "TB" || unit == "tb" || unit == "Tb") {
        return size * 1024L * 1024L * 1024L * 1024L;
    } else {
        throw std::invalid_argument("Unknown unit: " + unit);
    }
}

int main(int argc, char* argv[]) {
    long size;
    if (argc == 2) {
        std::string sizeStr(argv[1]);
        try {
            size = parseSize(sizeStr);
        } catch (const std::invalid_argument& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
    } else {
        size = DEFAULT_STORE_SIZE;
    }

    std::shared_ptr<LightningStore> store = std::make_shared<LightningStore>("/tmp/lightning", size);
    store->Run();

    return 0;
}