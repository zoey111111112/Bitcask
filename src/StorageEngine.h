#ifndef STORAGE_ENGINE_H
#define STORAGE_ENGINE_H

#include <string>
#include <unordered_map>
#include <cstdint>

struct KeyDirEntry { /* ... */ };
struct RecordHeader { /* ... */ };

class BitcaskEngine {
public:
    BitcaskEngine(const std::string& dir);
    ~BitcaskEngine();

    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    void del(const std::string& key);

private:
    std::string data_dir;
    std::string active_file;
    std::unordered_map<std::string, KeyDirEntry> keyDir;

    void load_data();
    void rotate_file_if_needed(); // 以后可以加这个：检查文件大小
};

#endif