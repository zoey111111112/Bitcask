#include "StorageEngine.h"
#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include "CRC32.h"




namespace fs = std::filesystem;
using std::cout,std::endl,std::string;

StorageEngine::StorageEngine(){
    load_data();
}
StorageEngine::~StorageEngine() {
    if (active_file_stream.is_open()) {
        active_file_stream.flush();
        active_file_stream.close();
    }
}

string StorageEngine::get(const string &key){
    // 获取读锁
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto it = keyDir.find(key);
    if(it == keyDir.end()){
        cout << "Key not found: " << key << endl;
        return "";
    }
    KeyDirEntry& entry = it->second; // 直接用迭代器拿数据，省去第二次查找

    string value;
    RecordHeader header;
    value.resize(entry.value_size);

    std::ifstream ifs(entry.file_id, std::ios::in | std::ios::binary);
    if (!ifs.is_open()) return "";
    // 1. 顺着读：Header -> Key -> Value
    ifs.seekg(entry.value_pos - key.size() - sizeof(RecordHeader));
    
    RecordHeader header;
    ifs.read(reinterpret_cast<char*>(&header), sizeof(header));

    string disk_key(header.key_size, 0);
    ifs.read(&disk_key[0], header.key_size);

    string value(header.value_size, 0);
    ifs.read(&value[0], header.value_size);

    // 2. 校验
    uint32_t check_crc = CRC32::Init();
    check_crc = CRC32::Update(check_crc, &header.timestamp, sizeof(uint32_t));
    check_crc = CRC32::Update(check_crc, &header.key_size, sizeof(uint32_t));
    check_crc = CRC32::Update(check_crc, &header.value_size, sizeof(uint32_t));
    check_crc = CRC32::Update(check_crc, disk_key.data(), disk_key.size());
    check_crc = CRC32::Update(check_crc, value.data(), value.size());
    
    if (CRC32::Final(check_crc) != header.crc) {
        std::cerr << "Data corruption detected for key: " << key << endl;
        return "";
    }

    cout << value << endl;

    return value;
}

void StorageEngine::put(const string &key, const string &value){
    // 获取写锁
    std::unique_lock<std::shared_mutex> lock(mutex_);

    uint32_t record_size = sizeof(RecordHeader) + key.size() + value.size();
    // 1. 检查是否需要切换文件
    if (!active_file_stream.is_open() || current_offset + record_size > MAX_FILE_SIZE) {
        rotate_file();

    }
    RecordHeader header;
    header.timestamp = static_cast<uint32_t>(time(nullptr));
    header.key_size = static_cast<uint32_t>(key.size());
    header.value_size = static_cast<uint32_t>(value.size());

    // --- 流式计算 CRC ---
    uint32_t crc = CRC32::Init();
    crc = CRC32::Update(crc, &header.timestamp, sizeof(uint32_t));
    crc = CRC32::Update(crc, &header.key_size, sizeof(uint32_t));
    crc = CRC32::Update(crc, &header.value_size, sizeof(uint32_t));
    crc = CRC32::Update(crc, key.data(), key.size());
    crc = CRC32::Update(crc, value.data(), value.size());
    header.crc = CRC32::Final(crc); // 得到最终校验码

    // 记录 Value 的起始位置：当前偏移量 + Header + Key
    uint32_t val_pos = current_offset + sizeof(RecordHeader) + key.size();
    active_file_stream.write(reinterpret_cast<const char*>(&header), sizeof(header));
    active_file_stream.write(key.data(), key.size());
    active_file_stream.write(value.data(), value.size());
    active_file_stream.flush();


    // 更新内存中的keyDir
    KeyDirEntry entry;
    entry.file_id = data_dir + "/" + std::to_string(current_file_id) + ".data";
    entry.value_size = header.value_size;
    entry.value_pos = val_pos;
    entry.timestamp = header.timestamp;
    keyDir[key] = entry;

    // 更新全局偏移量
    current_offset += record_size;
}

void StorageEngine::del(const string &key){
    // 获取写锁
    std::unique_lock<std::shared_mutex> lock(mutex_);

    if(keyDir.find(key) == keyDir.end()){
        cout << "Key not found: " << key << endl;
        return;
    }

    uint32_t record_size = sizeof(RecordHeader) + key.size();
    // 1. 检查是否需要切换文件
    if (!active_file_stream.is_open() || current_offset + record_size > MAX_FILE_SIZE) {
        rotate_file();
    }
    
    // 写入日志
    RecordHeader header;
    header.timestamp = static_cast<uint32_t>(time(nullptr));
    header.key_size = static_cast<uint32_t>(key.size());
    header.value_size = 0; // 删除标记

    // 计算 CRC（不包含 Value，因为没有 Value）
    uint32_t crc = CRC32::Init();
    crc = CRC32::Update(crc, &header.timestamp, sizeof(uint32_t));
    crc = CRC32::Update(crc, &header.key_size, sizeof(uint32_t));
    crc = CRC32::Update(crc, &header.value_size, sizeof(uint32_t));
    crc = CRC32::Update(crc, key.data(), key.size());
    header.crc = CRC32::Final(crc);

    active_file_stream.write(reinterpret_cast<const char*>(&header), sizeof(header));
    active_file_stream.write(key.data(), key.size());
    active_file_stream.flush();

    // 删除内存中的key
    keyDir.erase(key);

     // 更新全局偏移量
    current_offset += record_size;
}

bool sortFile(const fs::path& file1, const fs::path file2){
    string n1 = file1.stem().string(); // 使用 stem() 直接获取文件名去掉后缀的部分
    string n2 = file2.stem().string();
    return std::stoi(n1) < std::stoi(n2);

}

void StorageEngine::load_data(){
    // 检查是否存在data目录，不存在则创建
    // 检查目录下是否存在文件，不存在则创建1.data，并设置current_file_id = 1
    // 存在则超出最大的文件，并设置current_file_id
    // 将文件从小到大排序，依次打开检查文件，设置keyDir
    if(!fs::exists(data_dir)){
        if (fs::create_directory(data_dir)) {
            std::cout << "Data directory created: " << data_dir << std::endl;
        } else {
            std::cerr << "Failed to create data directory: " << data_dir << std::endl;
            // 错误处理，例如抛出异常或退出
        }
    }

    // 检查data目录是否为空
    if(fs::is_empty(data_dir)){
         std::cout << "Data directory is empty: " << data_dir << std::endl;
         
         current_file_id = 1;
         fs::path first_file = fs::path(data_dir) / (std::to_string(current_file_id) + ".data");
         
         std::ofstream ofs(first_file);
         if (ofs.is_open()) {
             std::cout << "Created initial data file: " << first_file << std::endl;
             ofs.close();
         } else {
             std::cerr << "Failed to create initial data file: " << first_file << std::endl;
         }
    }


    std::vector<fs::path> data_files;
    for(const auto& entry : fs::directory_iterator(data_dir)){
        if(entry.is_regular_file() && entry.path().extension() == ".data"){
            data_files.push_back(entry.path());
        }
    }

    std::sort(data_files.begin(), data_files.end(), sortFile);
    current_file_id = std::stoi(data_files.back().stem().string());


    for(const auto& file : data_files){
        std::ifstream ifs(file, std::ios::in | std::ios::binary);
        if(ifs.is_open()){
            RecordHeader header;
            string key,value;
            while(ifs.read(reinterpret_cast<char*>(&header), sizeof(header))){
                // 无论是否是删除标记，都必须先读取 key
                key.resize(header.key_size);
                ifs.read(&key[0], header.key_size);

                uint32_t check_crc = CRC32::Init();
                check_crc = CRC32::Update(check_crc, &header.timestamp, sizeof(uint32_t));
                check_crc = CRC32::Update(check_crc, &header.key_size, sizeof(uint32_t));
                check_crc = CRC32::Update(check_crc, &header.value_size, sizeof(uint32_t));
                check_crc = CRC32::Update(check_crc, key.data(), key.size());
               
                // 删除日志
                if(header.value_size == 0){
                    uint32_t actual_crc = CRC32::Final(check_crc);
                    if (actual_crc != header.crc) {
                        // 发现损坏记录，停止读取
                        break;
                    }
                    keyDir.erase(key);
                } else {
                    // 读取 Value
                    value.resize(header.value_size);
                    ifs.read(&value[0], header.value_size);

                    check_crc = CRC32::Update(check_crc, value.data(), value.size());
                    uint32_t actual_crc = CRC32::Final(check_crc);
                    if (actual_crc != header.crc) {
                        // 发现损坏记录，停止读取
                        break;
                    } 

                    KeyDirEntry entry;
                    entry.file_id = file.string(); // 显式转换为字符串
                    entry.timestamp = header.timestamp;
                    entry.value_pos = static_cast<uint32_t>(ifs.tellg()) - header.value_size;
                    entry.value_size = header.value_size;
                    keyDir[key] = entry;
                }

                // 无论是否是删除，都跳过 value 的长度（删除时 value_size 为 0，跳过 0 字节也没问题）
                // ifs.seekg(header.value_size, std::ios::cur);
            }
        }
        ifs.close();
    }

}

void StorageEngine::rotate_file() {
    if (active_file_stream.is_open()) {
        active_file_stream.close();
        current_file_id++;
    }
    string path = data_dir + "/" + std::to_string(current_file_id) + ".data";
    active_file_stream.open(path, std::ios::app | std::ios::binary);
    current_offset = fs::exists(path) ? fs::file_size(path) : 0;

     // 递归调用一下，或者直接再循环一次切换到 2.data
    if (current_offset >= MAX_FILE_SIZE) {
        rotate_file(); 
    }
}