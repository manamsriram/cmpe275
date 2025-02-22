#include "CSV.h"
#include <iostream>

void CSV::addRow(const CSVRow &row) { rows.push_back(row); }

const CSVRow &CSV::getRow(size_t index) const { return rows.at(index); }

size_t CSV::rowCount() const { return rows.size(); }

void CSV::printHead(size_t lines) const {
  for (size_t i = 0; i < lines && i < rows.size(); ++i) {
    rows[i].printRow();
  }
}

void CSV::printRow(size_t index) const {
    if (index < rows.size()) {
      rows[index].printRow();
    } else {
      std::cerr << "Index out of range" << std::endl;
    }
}
