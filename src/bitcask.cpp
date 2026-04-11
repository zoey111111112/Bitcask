#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <unordered_map>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include "common.h"
#include "StorageEngine.h"

/*
下一步计划：
1. 将单个文件限制在8MB
2.只有一个active文件
3.引入文件编号,1.data,2.data
4.增加CRC校验逻辑
5.增加后台merge进程生成hint file
*/

using std::string,std::cin,std::cout,std::cerr,std::endl;
using std::istringstream, std::fstream;



int main(int argc,char* argv[]){

    string line;
    string cmd,key,value;
    StorageEngine engine;
    
    while(getline(cin,line)){
        istringstream iss(line);
        iss >> cmd >> key >> value;

        if(cmd == "put") {
            engine.put(key, value);
        } else if(cmd == "get") {
            engine.get(key);
        } else if(cmd == "del") {
            engine.del(key);
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

