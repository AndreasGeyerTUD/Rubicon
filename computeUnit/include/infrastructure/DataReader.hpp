#pragma once

#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <vector>

#include "Column.hpp"
#include "DataCatalog.hpp"
#include "Logger.hpp"

namespace datareader {

// https://stackoverflow.com/a/217605
// trim from start (in place)
inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
};

// trim from end (in place)
inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
                return !std::isspace(ch);
            }).base(),
            s.end());
};

// trim from both ends (in place)
inline void trim(std::string& s) {
    rtrim(s);
    ltrim(s);
};

std::map<std::string, std::vector<std::pair<std::string, std::string>>> readSchemaFile_regex(const std::string& path) {
    std::regex re("([a-zA-Z]+_[a-zA-Z]+)\\(([a-z]+)\\)");
    std::smatch m;
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> schema;

    std::ifstream file(path);
    if (file.is_open()) {
        std::string line;
        while (getline(file, line)) {
            LOG_DEBUG1("Parsing line: " << line << std::endl;)
            const size_t splitpos = line.find_first_of(":");
            std::string tablename = line.substr(0, splitpos);
            std::vector<std::pair<std::string, std::string>> columns;
            while (std::regex_search(line, m, re)) {
                // Emplace ColumnName / ColumnType pair
                LOG_DEBUG1("Adding" << m[1] << ": " << m[2] << " to the column set" << std::endl;)
                columns.emplace_back(m[1], m[2]);
                // Truncate line to keep searching for more matches
                line = m.suffix().str();
            }
            LOG_DEBUG1("Adding Table " << tablename << std::endl;)
            schema.emplace(tablename, columns);
        }
    } else {
        LOG_ERROR("[readSchemaFile] Could not open file: " << path << std::endl;)
    }
    return schema;
};

std::map<std::string, std::vector<std::pair<std::string, std::string>>> readSchemaFile(const std::string& path) {
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> schema;

    std::ifstream file(path);
    if (file.is_open()) {
        std::string line;
        while (getline(file, line)) {
            trim(line);
            if (line.size() == 0 || (line.size() > 0 && line[0] == '#') || (line.size() > 1 && line[0] == '/' && line[1] == '/')) {  // ignore comment or empty line
                continue;
            }
            size_t pos = line.find_first_of(":");
            std::string tablename = line.substr(0, pos);
            trim(tablename);
            std::string colStrings = line.substr(pos + 1, std::string::npos);
            std::vector<std::pair<std::string, std::string>> columns;

            while (colStrings.size() > 0) {
                size_t posIn = colStrings.find(",");
                if (posIn == std::string::npos) {
                    posIn = colStrings.find(";");
                }

                std::string cur = colStrings.substr(0, posIn);
                std::string colName = cur.substr(0, cur.find("("));
                trim(colName);
                std::string type = cur.substr(cur.find("(") + 1, cur.find(")") - cur.find("(") - 1);  // Getting the type string and exclude the ()
                trim(type);

                colStrings = colStrings.substr(posIn + 1, std::string::npos);

                columns.push_back(std::make_pair(colName, type));
            }
            schema.emplace(tablename, columns);
        }
        file.close();
    } else {
        LOG_ERROR("[readSchemaFile] Could not open file: " << path << std::endl;)
    }
    LOG_INFO("[readSchemaFile] Done." << std::endl;)
    return schema;
}

void readDataFile_binary(const std::string& path, const std::string& tablename, const std::vector<std::pair<std::string, std::string>>& columns, const ssize_t numaNodeId = -1) {
    auto getFileSize = [](std::ifstream& file) -> size_t {
        if (file.is_open()) {
            file.seekg(0, std::ios::end);
            const size_t size = file.tellg();
            file.seekg(0, std::ios::beg);
            file.clear();
            return size;
        } else {
            LOG_ERROR("[readDataFile] Could not read data file!" << std::endl;)
            return 0;
        }
    };

    auto readColumn = [&](std::string colName, std::string colType) { 
        DataType datatype = DataType::unknown;
        size_t elements = 0;

        std::ifstream binary_data(path + "/" + tablename + "/" + colName + ".bin", std::ios::binary);
        const size_t binary_data_size = getFileSize(binary_data);

        if (colType == "int") {
            datatype = DataType::int64;
            elements = binary_data_size / sizeof(int64_t);
        } else if (colType == "str") {
            datatype = DataType::string_enc;
            elements = binary_data_size / sizeof(uint64_t);
        } else if (colType == "bool") {
            datatype = DataType::uint8;
            elements = binary_data_size;
        } else if (colType == "date") {
            datatype = DataType::timestamp;
            elements = binary_data_size / sizeof(uint64_t);
        } else {
            LOG_ERROR("[readDataFile] Unknown colType detected. What should i do?" << std::endl;)
            exit(-1);
        }

        std::shared_ptr<Column> catalog_column = std::make_shared<Column>(0, datatype, 0, elements, false, true, numaNodeId);
        catalog_column->allocate();
        // LOG_DEBUG1("[readDataFile] Reading data into column buffer." << std::endl;)
        binary_data.read(static_cast<char*>(catalog_column->data), binary_data_size);

        if (colType == "str") {
            std::ifstream dictionary(path + "/" + tablename + "/" + colName + "_dict.tsv");
            if (dictionary.is_open()) {
                std::string line;
                while (getline(dictionary, line)) {
                    char* end = nullptr;
                    auto sep = line.find_first_of("\t");
                    const std::string str = line.substr(0, sep);
                    const std::string val = line.substr(sep + 1, std::string::npos);
                    const uint64_t encoded = std::strtoull(val.c_str(), &end, 10);
                    catalog_column->dictionaryEncoding->encodeValue(str);
                    const uint64_t catalog_encoding = catalog_column->dictionaryEncoding->getDictionaryValue(str);
                    if (catalog_encoding != encoded) {
                        LOG_ERROR("[readDataFile] Wrong encoded value for " << str << ". Expected " << encoded << " but got " << catalog_encoding << std::endl;)
                    }
                }
                catalog_column->dictionaryEncoding->setReady();
                LOG_DEBUG1("[readDataFile] Adding column: " << colName << " to table: " << tablename << " with dict size: " << catalog_column->dictionaryEncoding->getDictionarySize() << std::endl;)
            }
        } else {
            LOG_DEBUG1("[readDataFile] Adding column: " << colName << " to table: " << tablename << std::endl;)
        }
        DataCatalog::getInstance().addColumn(tablename, colName, catalog_column);
    };

    std::vector<std::jthread> threads;
    threads.reserve(columns.size());

    for (auto col : columns) {
        threads.emplace_back(readColumn, col.first, col.second);
    }
}

void readDataFile(const std::string& path, const std::string& tablename, const std::vector<std::pair<std::string, std::string>>& columns, const ssize_t numaNodeId = -1) {
    std::map<std::string, std::vector<std::string>> data;

    std::ifstream file(path + "/" + tablename + ".tbl");
    if (file.is_open()) {
        // https://www.reddit.com/r/cpp_questions/comments/11wlf49/comment/jcyliow/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button
        auto numberLines = std::count_if(std::istreambuf_iterator<char>{file}, {}, [](char c) { return c == '\n'; });

        std::vector<std::shared_ptr<Column>> locColumns;
        std::vector<std::string> locColumnsNames;

        for (auto col : columns) {
            std::string& colName = col.first;
            std::string& colType = col.second;

            DataType datatype = DataType::unknown;

            if (colType == "int") {
                datatype = DataType::int64;
            } else if (colType == "str") {
                datatype = DataType::string_enc;
            } else if (colType == "bool") {
                datatype = DataType::uint8;
            } else if (colType == "date") {
                datatype = DataType::timestamp;
            }

            std::shared_ptr<Column> resColumn = std::make_shared<Column>(0, datatype, 0, numberLines, false, true, numaNodeId);
            resColumn->allocate();

            locColumns.push_back(resColumn);
            locColumnsNames.push_back(colName);
        }

        // Need to reset the file to the beginning -> https://stackoverflow.com/a/7681612
        file.clear();                  // clear fail and eof bits
        file.seekg(0, std::ios::beg);  // back to the start!

        size_t lineCount = 0;
        std::string line;

        while (getline(file, line)) {
            trim(line);
            if (line.size() == 0 || (line.size() > 0 && line[0] == '#') || (line.size() > 1 && line[0] == '/' && line[1] == '/')) {  // ignore comment or empty line
                continue;
            }

            size_t colCount = 0;
            while (line.size() > 0) {
                size_t posIn = line.find("|");
                if (posIn == std::string::npos) {
                    posIn = line.find(" \n");
                }

                std::string cur = line.substr(0, posIn);

                const DataType datatype = locColumns[colCount]->datatype;

                if (datatype == DataType::int64) {
                    const int64_t val = std::stoll(cur);
                    std::memcpy(static_cast<char*>(locColumns[colCount]->data) + (lineCount * sizeof(val)), &val, sizeof(val));
                } else if (datatype == DataType::string_enc) {
                    const uint64_t val = locColumns[colCount]->dictionaryEncoding->encodeValue(cur);
                    std::memcpy(static_cast<char*>(locColumns[colCount]->data) + (lineCount * sizeof(val)), &val, sizeof(val));
                } else if (datatype == DataType::uint8) {
                    const int8_t val = std::stoi(cur);
                    std::memcpy(static_cast<char*>(locColumns[colCount]->data) + (lineCount * sizeof(val)), &val, sizeof(val));
                } else if (datatype == DataType::timestamp) {
                    std::tm t = {};
                    std::istringstream ss(cur);
                    ss >> std::get_time(&t, "%Y%m%d");

                    if (ss.fail()) {
                        throw std::runtime_error("Failed to parse time string: " + cur);
                    }

                    const uint64_t timestamp = std::mktime(&t);  // Could be divided by 100 without information loss because of day granularity
                    std::memcpy(static_cast<char*>(locColumns[colCount]->data) + (lineCount * sizeof(timestamp)), &timestamp, sizeof(timestamp));
                } else {
                    LOG_ERROR("[readDataFile] Unsupported datatype: " << static_cast<int>(datatype) << std::endl;)
                }

                line = line.substr(posIn + 1, std::string::npos);

                ++colCount;
            }

            ++lineCount;
        }

        file.close();

        for (size_t i = 0; i < locColumns.size(); ++i) {
            if (locColumns[i]->datatype == DataType::string_enc) {
                locColumns[i]->dictionaryEncoding->setReady();
                LOG_DEBUG1("[readDataFile] Adding column: " << locColumnsNames[i] << " to table: " << tablename << " with dict size: " << locColumns[i]->dictionaryEncoding->getDictionarySize() << std::endl;)
            } else {
                LOG_DEBUG1("[readDataFile] Adding column: " << locColumnsNames[i] << " to table: " << tablename << std::endl;)
            }

            DataCatalog::getInstance().addColumn(tablename, locColumnsNames[i], locColumns[i]);
        }
    } else {
        LOG_ERROR("[readDataFile] Could not open file: " << path + "/" + tablename + ".tbl" << std::endl;)
    }
}

void ingestSSBData(const std::string& path, const std::string& loadtype, const ssize_t numaNodeId = -1) {
    // load schema file (should be same dir as data)
    std::map<std::string, std::vector<std::pair<std::string, std::string>>> schema = readSchemaFile_regex(path + "/schema.txt");
    
    if (loadtype == "bin") {
        LOG_INFO("[ingestSSB] Ingesting data using BINARY basedata" << std::endl;)
        for (const auto& table : schema) {
            readDataFile_binary(path, table.first, table.second, numaNodeId);
        }
    } else {
        LOG_INFO("[ingestSSB] Ingesting data using CSV basedata" << std::endl;)
        for (const auto& table : schema) {
            readDataFile(path, table.first, table.second);
        }
    }
}

}  // namespace datareader