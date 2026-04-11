#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <string>
#include <unordered_map>
#include <cstdint>
#include <fstream>
#include <filesystem> // 如果你使用了 fs::path 也可以加上
#include "common.h"

class StorageEngine {
public:
    StorageEngine();
    ~StorageEngine();
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    void del(const std::string& key);

private:
    std::string data_dir = "data";
    std::uint32_t current_file_id = 1;
    uint32_t current_offset = 0; // 记录当前活跃文件的写入位置
    std::unordered_map<std::string, KeyDirEntry> keyDir;
    std::uint32_t MAX_FILE_SIZE = 1024 * 8; // 8MB
    std::ofstream active_file_stream; // 保持活跃文件的长连接

    void rotate_file(); // 封装文件切换逻辑
    void load_data();
};

#endif