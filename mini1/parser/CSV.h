#ifndef CSV_H
#define CSV_H

#include <vector>
#include "CSVRow.h"

class CSV {
public:
  std::vector<CSVRow> rows;

  void addRow(const CSVRow &row);

  const CSVRow &getRow(size_t index) const;

  size_t rowCount() const;

  void printHead(size_t lines = 5) const;

  void printRow(size_t index) const;
};

#endif
