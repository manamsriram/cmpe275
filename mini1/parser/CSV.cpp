#include "CSV.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

inline double parseDouble(const std::string &field) {
  if (field.empty() || field == "NULL" || field == "Unspecified")
      return 0.0;
  return std::stod(field);
}

inline int parseInt(const std::string &field) {
  if (field.empty() || field == "NULL" || field == "Unspecified")
      return 0;
  return std::stoi(field);
}

inline std::string parseString(const std::string &field) {
  if (field.empty() || field == "NULL" || field == "Unspecified")
      return "";
  return field;
}

size_t CSV::size() const {
  // Return the size of one of your vectors, e.g.:
  return crash_dates.size();
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
  if (!std::getline(file, line)) {
      return data;
  }

  while (std::getline(file, line)) {
      std::istringstream s(line);
      std::string field;

      try {
          std::getline(s, field, ',');
          data.crash_dates.push_back(parseString(field));

          std::getline(s, field, ',');
          data.crash_times.push_back(parseString(field));

          std::getline(s, field, ',');
          data.boroughs.push_back(parseString(field));

          std::getline(s, field, ',');
          data.zip_codes.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.latitudes.push_back(parseDouble(field));

          std::getline(s, field, ',');
          data.longitudes.push_back(parseDouble(field));

          std::getline(s, field, ',');
          data.locations.push_back(parseString(field));

          std::getline(s, field, ',');
          data.on_street_names.push_back(parseString(field));

          std::getline(s, field, ',');
          data.cross_street_names.push_back(parseString(field));

          std::getline(s, field, ',');
          data.off_street_names.push_back(parseString(field));

          std::getline(s, field, ',');
          data.persons_injured.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.persons_killed.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.pedestrians_injured.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.pedestrians_killed.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.cyclists_injured.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.cyclists_killed.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.motorists_injured.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.motorists_killed.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.contributing_factor_vehicle_1.push_back(parseString(field));

          std::getline(s, field, ',');
          data.contributing_factor_vehicle_2.push_back(parseString(field));

          std::getline(s, field, ',');
          data.contributing_factor_vehicle_3.push_back(parseString(field));

          std::getline(s, field, ',');
          data.contributing_factor_vehicle_4.push_back(parseString(field));

          std::getline(s, field, ',');
          data.contributing_factor_vehicle_5.push_back(parseString(field));

          std::getline(s, field, ',');
          data.collision_ids.push_back(parseInt(field));

          std::getline(s, field, ',');
          data.vehicle_type_code_1.push_back(parseString(field));

          std::getline(s, field, ',');
          data.vehicle_type_code_2.push_back(parseString(field));

          std::getline(s, field, ',');
          data.vehicle_type_code_3.push_back(parseString(field));

          std::getline(s, field, ',');
          data.vehicle_type_code_4.push_back(parseString(field));

          std::getline(s, field, ',');
          data.vehicle_type_code_5.push_back(parseString(field));

      } catch (const std::exception &e) {
          ignoredRows++;
      }
  }

  std::cout << "Ignored Rows = " << ignoredRows << std::endl;
  std::cout << "Number of rows successfully parsed: " << data.size() << std::endl;
  return data;
}