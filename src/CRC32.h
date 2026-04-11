#ifndef CRC32_H
#define CRC32_H

#include <array>
#include <cstdint>
#include <cstddef>

class CRC32 {
public:
    // 1. 初始化 CRC 值 (标准起点是 0xFFFFFFFF)
    static uint32_t Init() {
        return 0xFFFFFFFF;
    }

    // 2. 流式更新：在原有 CRC 基础上增加一段数据
    static uint32_t Update(uint32_t crc, const void* data, size_t len) {
        const uint8_t* byte_data = reinterpret_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; ++i) {
            // CRC32 核心计算逻辑：查表并异或
            crc = (crc >> 8) ^ GetTable()[(crc ^ byte_data[i]) & 0xFF];
        }
        return crc;
    }

    // 3. 结束计算：进行最后的取反操作
    static uint32_t Final(uint32_t crc) {
        return crc ^ 0xFFFFFFFF;
    }

    // 4. 便捷方法：一次性计算完整数据的 CRC
    static uint32_t Calculate(const void* data, size_t len) {
        return Final(Update(Init(), data, len));
    }

private:
    // 静态查表：在第一次调用时生成，后续直接复用
    static const std::array<uint32_t, 256>& GetTable() {
        static const auto table = []() {
            std::array<uint32_t, 256> t;
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t crc = i;
                for (uint32_t j = 0; j < 8; ++j) {
                    if (crc & 1) crc = (crc >> 1) ^ 0xEDB88320; // 标准多项式
                    else crc >>= 1;
                }
                t[i] = crc;
            }
            return t;
        }();
        return table;
    }
};

#endif