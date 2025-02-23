#include "CSVParser.h"
#include <fstream>
#include <sstream>
#include <iostream>

CSVParser::CSVParser(const std::string& filename) : filename(filename), rowCount(0) {}

void CSVParser::parse() {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::vector<std::string> tokens;
        tokenize(line, tokens, ',');

        if (columns.empty()) {
            columns.resize(tokens.size());
        }

        for (size_t i = 0; i < tokens.size(); ++i) {
            columns[i].push_back(tokens[i]);
        }

        ++rowCount;
    }
}

size_t CSVParser::getRowCount() const {
    return rowCount;
}

const std::vector<std::string>& CSVParser::getColumn(size_t index) const {
    return columns[index];
}

void CSVParser::tokenize(const std::string& str, std::vector<std::string>& tokens, char delim) {
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delim)) {
        tokens.push_back(token);
    }
}
