#ifndef COMMON_H
#define COMMON_H
#include <cstdint>
#include <string>


struct KeyDirEntry {
    std::string file_id;
    uint32_t value_size;
    uint32_t value_pos;
    uint32_t timestamp;
};

struct RecordHeader {
    uint32_t crc;
    uint32_t timestamp;
    uint32_t key_size;
    uint32_t value_size;
};

#endif