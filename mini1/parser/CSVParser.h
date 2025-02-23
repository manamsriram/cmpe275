#ifndef CSV_PARSER_H
#define CSV_PARSER_H

#include <string>
#include <vector>

class CSVParser {
public:
    CSVParser(const std::string& filename);
    void parse();
    size_t getRowCount() const;
    const std::vector<std::string>& getColumn(size_t index) const;

private:
    std::string filename;
    std::vector<std::vector<std::string>> columns;
    size_t rowCount;

    void tokenize(const std::string& str, std::vector<std::string>& tokens, char delim);
};

#endif
