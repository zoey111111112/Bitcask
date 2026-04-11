#include "StorageEngine.h"
#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>



namespace fs = std::filesystem;
using std::cout,std::endl,std::string;

StorageEngine::StorageEngine(){
    load_data();
}

void StorageEngine::get(const string &key){
    if(keyDir.find(key) == keyDir.end()){
        cout << "Key not found: " << key << endl;
        return;
    }
    KeyDirEntry entry = keyDir[key];
    string value;
    value.resize(entry.value_size);

    std::ifstream ifs(entry.file_id, std::ios::in | std::ios::binary);
    ifs.seekg(entry.value_pos);
    ifs.read(&value[0], entry.value_size);
    ifs.close();

    cout << value << endl;
}

void StorageEngine::put(const string &key, const string &value){
    RecordHeader header;
    header.crc = 0;
    header.timestamp = static_cast<uint32_t>(time(nullptr));
    header.key_size = key.size();
    header.value_size = value.size();

    string active_file = data_dir + "/" + std::to_string(current_file_id) + ".data";
    if(fs::file_size(active_file) + sizeof(header) + key.size() + value.size() > max_file_size){
        current_file_id++;
        active_file = data_dir + "/" + std::to_string(current_file_id) + ".data";
    }
    std::ofstream ofs(active_file, std::ios::app | std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(&header), sizeof(header));
    ofs.write(key.data(), key.size());
    ofs.write(value.data(), value.size());
    uint32_t offset = static_cast<uint32_t>(ofs.tellp());
    ofs.flush();
    ofs.close();

    // 更新内存中的keyDir
    KeyDirEntry entry;
    entry.file_id = data_dir + "/" + std::to_string(current_file_id) + ".data";
    entry.value_size = header.value_size;
    entry.value_pos = offset - header.value_size;
    entry.timestamp = header.timestamp;
    keyDir[key] = entry;
}

void StorageEngine::del(const string &key){
    if(keyDir.find(key) == keyDir.end()){
        cout << "Key not found: " << key << endl;
        return;
    }
    RecordHeader header;
    header.crc = 0;
    header.timestamp = static_cast<uint32_t>(time(nullptr));
    header.key_size = key.size();
    header.value_size = 0;

    string active_file = data_dir + "/" + std::to_string(current_file_id) + ".data";
    if(fs::file_size(active_file) + sizeof(header) + key.size() > max_file_size){
        current_file_id++;
        active_file = data_dir + "/" + std::to_string(current_file_id) + ".data";
    }

    std::ofstream ofs(active_file, std::ios::app | std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(&header), sizeof(header));
    ofs.write(key.data(), key.size());
    ofs.flush();
    ofs.close();

    // 删除内存中的key
    keyDir.erase(key);
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
    current_file_id = std::stoi(data_files.back().filename().string().substr(0, data_files.back().filename().string().length() - 5));


    for(const auto& file : data_files){
        std::ifstream ifs(file);
        if(ifs.is_open()){
            RecordHeader header;
            string key,value;
            while(ifs.read(reinterpret_cast<char*>(&header), sizeof(header))){
                // 无论是否是删除标记，都必须先读取 key
                key.resize(header.key_size);
                ifs.read(&key[0], header.key_size);

                // 删除日志
                if(header.value_size == 0){
                    keyDir.erase(key);
                    continue;
                }

                value.resize(header.value_size);
                ifs.read(&value[0], header.value_size);

                KeyDirEntry entry;
                entry.file_id = file.string(); // 显式转换为字符串
                entry.timestamp = header.timestamp;
                entry.value_pos = static_cast<uint32_t>(ifs.tellg()) - header.value_size;
                entry.value_size = header.value_size;
                keyDir[key] = entry;
            }
        }
        ifs.close();
    }

}