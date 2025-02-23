#include "CSV.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

inline double parseDouble(const std::string &field) {
  if (field.empty() || field == "NULL" || field == "Unspecified")
    return std::numeric_limits<double>::quiet_NaN();
  try {
    return std::stod(field);
  } catch (const std::exception &) {
    return std::numeric_limits<double>::quiet_NaN();
  }
}

inline int parseInt(const std::string &field) {
  if (field.empty() || field == "NULL" || field == "Unspecified")
    return std::numeric_limits<int>::min();
  try {
    return std::stoi(field);
  } catch (const std::exception &) {
    return std::numeric_limits<int>::min();
  }
}

inline std::string parseString(const std::string &field) {
  return field.empty() || field == "NULL" || field == "Unspecified" ? ""
                                                                    : field;
}

std::string stripQuotes(const std::string &field) {
  if (field.size() >= 2 && field.front() == '"' && field.back() == '"') {
    return field.substr(1, field.size() - 2);
  }
  return field;
}

size_t CSV::size() const { return crash_dates.size(); }

std::vector<std::string> splitCSVLine(const std::string &line) {
  std::vector<std::string> fields;
  std::string field;
  bool inQuotes = false;
  for (char c : line) {
    if (c == '"') {
      inQuotes = !inQuotes;
    } else if (c == ',' && !inQuotes) {
      fields.push_back(stripQuotes(field));
      field.clear();
    } else {
      field += c;
    }
  }
  fields.push_back(stripQuotes(field));
  return fields;
}

CSV makeCSV(const std::string &filename) {
  int ignoredRows = 0;
  CSV data;
  std::ifstream file(filename);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return data;
  }

  std::string line;

  while (std::getline(file, line)) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 29) {
      ignoredRows++;
      continue;
    }

    try {
      data.crash_dates.push_back(parseString(fields[0]));
      data.crash_times.push_back(parseString(fields[1]));
      data.boroughs.push_back(parseString(fields[2]));
      data.zip_codes.push_back(parseInt(fields[3]));
      data.latitudes.push_back(parseDouble(fields[4]));
      data.longitudes.push_back(parseDouble(fields[5]));
      data.locations.push_back(parseString(fields[6]));
      data.on_street_names.push_back(parseString(fields[7]));
      data.cross_street_names.push_back(parseString(fields[8]));
      data.off_street_names.push_back(parseString(fields[9]));
      data.persons_injured.push_back(parseInt(fields[10]));
      data.persons_killed.push_back(parseInt(fields[11]));
      data.pedestrians_injured.push_back(parseInt(fields[12]));
      data.pedestrians_killed.push_back(parseInt(fields[13]));
      data.cyclists_injured.push_back(parseInt(fields[14]));
      data.cyclists_killed.push_back(parseInt(fields[15]));
      data.motorists_injured.push_back(parseInt(fields[16]));
      data.motorists_killed.push_back(parseInt(fields[17]));
      data.contributing_factor_vehicle_1.push_back(parseString(fields[18]));
      data.contributing_factor_vehicle_2.push_back(parseString(fields[19]));
      data.contributing_factor_vehicle_3.push_back(parseString(fields[20]));
      data.contributing_factor_vehicle_4.push_back(parseString(fields[21]));
      data.contributing_factor_vehicle_5.push_back(parseString(fields[22]));
      data.collision_ids.push_back(parseInt(fields[23]));
      data.vehicle_type_code_1.push_back(parseString(fields[24]));
      data.vehicle_type_code_2.push_back(parseString(fields[25]));
      data.vehicle_type_code_3.push_back(parseString(fields[26]));
      data.vehicle_type_code_4.push_back(parseString(fields[27]));
      data.vehicle_type_code_5.push_back(parseString(fields[28]));
    } catch (const std::exception &e) {
      ignoredRows++;
    }
  }

  std::cout << "Ignored Rows = " << ignoredRows << std::endl;
  std::cout << "Number of rows successfully parsed: " << data.size()
            << std::endl;
  return data;
}