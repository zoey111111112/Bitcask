#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <string>
#include <unordered_map>
#include <cstdint>
#include "common.h"

class StorageEngine {
public:
    StorageEngine();
    void put(const std::string& key, const std::string& value);
    void get(const std::string& key);
    void del(const std::string& key);

private:
    std::string data_dir = "data";
    std::uint8_t current_file_id;
    std::unordered_map<std::string, KeyDirEntry> keyDir;
    std::uint32_t max_file_size = 1024 * 8; // 8MB
    void load_data();
};

#endif