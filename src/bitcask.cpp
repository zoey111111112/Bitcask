#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <unordered_map>
#include <cstdint>
#include <ctime>
#include <cstdlib>


using std::string,std::cin,std::cout,std::cerr,std::endl;
using std::istringstream, std::fstream;

namespace fs = std::filesystem;

struct RecordHeader {
    uint32_t crc;
    uint32_t timestamp;
    uint32_t key_size;
    uint32_t value_size;
};

void put(const string&,const string&);
void del(const string&);
void get(const string&);
void load_data();

string data_file = "data/data_file.data";


int main(int argc,char* argv[]){

    string line;
    string cmd,key,value;
    load_data();
    
    while(getline(cin,line)){
        istringstream iss(line);
        iss >> cmd >> key >> value;

        if(cmd == "put") {
            put(key, value);
        } else if(cmd == "get") {
            get(key);
        } else if(cmd == "del") {
            del(key);
        } else if(cmd == "help") {
            cout << "Available commands: put, get, del, help, exit" << endl;
        } else if(cmd == "exit"){
            exit(0);
        } else {
            cerr << "Unknown command: " << cmd << endl;
        }

    }

    return 0;
}

void put(const string& key,const string& value) {
    fstream file(data_file, std::ios::app);
    if(file.is_open()){
        RecordHeader header;
        header.crc = 0;  // 计算CRC
        header.timestamp = static_cast<uint32_t>(time(nullptr));
        header.key_size = key.size();
        header.value_size = value.size();

        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        file.write(key.data(), key.size());
        file.write(value.data(), value.size());
        file.flush();
        file.close();
    } else {
        cerr << "Failed to open data file for writing." << endl;
    }

}
void del(const string& key) {
    fstream file(data_file, std::ios::app);
    if(file.is_open()){
        RecordHeader header;
        header.crc = 0;  // 计算CRC
        header.timestamp = static_cast<uint32_t>(time(nullptr));
        header.key_size = key.size();
        header.value_size = 0;

        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        file.write(key.data(), key.size());
        file.flush();
        file.close();
    } else {
        cerr << "Failed to open data file for writing." << endl;
    }

}
void get(const string& key) {
    fstream file(data_file, std::ios::in | std::ios::binary);
    bool deleted;
    string value;
    if(file.is_open()){
        RecordHeader header;
        string ekey,evalue;
        while(file.read(reinterpret_cast<char*>(&header), sizeof(header))){
            ekey.resize(header.key_size);
            evalue.resize(header.value_size);
            file.read(&ekey[0], header.key_size);
            file.read(&evalue[0], header.value_size);

            if(ekey == key){
                if(header.value_size == 0){
                    deleted = true;
                } else {
                    deleted = false;
                    value = evalue;
                }
            }
        }
    } else {
        cerr << "Failed to open data file for reading." << endl;
    }
    if(deleted || value == ""){
        cout << "key not existed" << endl;
        return;
    }
    cout << value << endl;
}

void load_data() {
    /* 检查当前路径下是否存在data目录 */
    fs::path data_dir = "data";
    if(!fs::exists(data_dir)){
        if(fs::create_directory(data_dir)){
            cout << "Data directory created." << endl;
        } else {
            cout << "Failed to create data directory." << endl;
        }
    }
    /* 检查data目录下是否存在data_file文件 */
    fs::path file_path = data_dir / "data_file.data";
    if(!fs::exists(file_path)){
        std::ofstream file(file_path);
        if(file){
            cout << "Data file created." << endl;
        } else {    
            cout << "Failed to create data file." << endl;
        }
    }

    /* 从data_file中构建keyDir */
    fstream file(data_file, std::ios::in | std::ios::binary);
    if(file.is_open()){
        RecordHeader header;
        string ekey,evalue;
        while(file.read(reinterpret_cast<char*>(&header), sizeof(header))){
            ekey.resize(header.key_size);
            evalue.resize(header.value_size);
            file.read(&ekey[0], header.key_size);
            file.read(&evalue[0], header.value_size);
            if(header.value_size != 0){
                cout << ekey << " " << evalue << endl;
            } else {
                cout << ekey << " deleted" << endl;
            }
        }
    } else {
        cerr << "Failed to open data file for reading." << endl;
    }
}