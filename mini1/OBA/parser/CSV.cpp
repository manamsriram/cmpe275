#include "CSV.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>
#include <omp.h>

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
  CSV data;
  std::ifstream file(filename);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return data;
  }

  std::vector<std::string> lines;
  std::string line;
  while (std::getline(file, line)) {
    lines.push_back(line);
  }

  int ignoredRows = 0;
  int num_threads = omp_get_max_threads();
  std::vector<CSV> thread_data(num_threads);

  #pragma omp parallel for reduction(+:ignoredRows)
  for (size_t i = 0; i < lines.size(); ++i) {
    auto fields = splitCSVLine(lines[i]);
    if (fields.size() < 29) {
      ignoredRows++;
      continue;
    }

    try {
      int tid = omp_get_thread_num();
      thread_data[tid].crash_dates.push_back(parseString(fields[0]));
      thread_data[tid].crash_times.push_back(parseString(fields[1]));
      thread_data[tid].boroughs.push_back(parseString(fields[2]));
      thread_data[tid].zip_codes.push_back(parseInt(fields[3]));
      thread_data[tid].latitudes.push_back(parseDouble(fields[4]));
      thread_data[tid].longitudes.push_back(parseDouble(fields[5]));
      thread_data[tid].locations.push_back(parseString(fields[6]));
      thread_data[tid].on_street_names.push_back(parseString(fields[7]));
      thread_data[tid].cross_street_names.push_back(parseString(fields[8]));
      thread_data[tid].off_street_names.push_back(parseString(fields[9]));
      thread_data[tid].persons_injured.push_back(parseInt(fields[10]));
      thread_data[tid].persons_killed.push_back(parseInt(fields[11]));
      thread_data[tid].pedestrians_injured.push_back(parseInt(fields[12]));
      thread_data[tid].pedestrians_killed.push_back(parseInt(fields[13]));
      thread_data[tid].cyclists_injured.push_back(parseInt(fields[14]));
      thread_data[tid].cyclists_killed.push_back(parseInt(fields[15]));
      thread_data[tid].motorists_injured.push_back(parseInt(fields[16]));
      thread_data[tid].motorists_killed.push_back(parseInt(fields[17]));
      thread_data[tid].contributing_factor_vehicle_1.push_back(parseString(fields[18]));
      thread_data[tid].contributing_factor_vehicle_2.push_back(parseString(fields[19]));
      thread_data[tid].contributing_factor_vehicle_3.push_back(parseString(fields[20]));
      thread_data[tid].contributing_factor_vehicle_4.push_back(parseString(fields[21]));
      thread_data[tid].contributing_factor_vehicle_5.push_back(parseString(fields[22]));
      thread_data[tid].collision_ids.push_back(parseInt(fields[23]));
      thread_data[tid].vehicle_type_code_1.push_back(parseString(fields[24]));
      thread_data[tid].vehicle_type_code_2.push_back(parseString(fields[25]));
      thread_data[tid].vehicle_type_code_3.push_back(parseString(fields[26]));
      thread_data[tid].vehicle_type_code_4.push_back(parseString(fields[27]));
      thread_data[tid].vehicle_type_code_5.push_back(parseString(fields[28]));
    } catch (const std::exception &e) {
      ignoredRows++;
    }
  }

  for (int tid = 0; tid < num_threads; ++tid) { // Use index-based loop
    data.crash_dates.insert(data.crash_dates.end(), thread_data[tid].crash_dates.begin(), thread_data[tid].crash_dates.end());
    data.crash_times.insert(data.crash_times.end(), thread_data[tid].crash_times.begin(), thread_data[tid].crash_times.end());
    data.boroughs.insert(data.boroughs.end(), thread_data[tid].boroughs.begin(), thread_data[tid].boroughs.end());
    data.zip_codes.insert(data.zip_codes.end(), thread_data[tid].zip_codes.begin(), thread_data[tid].zip_codes.end());
    data.latitudes.insert(data.latitudes.end(), thread_data[tid].latitudes.begin(), thread_data[tid].latitudes.end());
    data.longitudes.insert(data.longitudes.end(), thread_data[tid].longitudes.begin(), thread_data[tid].longitudes.end());
    data.locations.insert(data.locations.end(), thread_data[tid].locations.begin(), thread_data[tid].locations.end());
    data.on_street_names.insert(data.on_street_names.end(), thread_data[tid].on_street_names.begin(), thread_data[tid].on_street_names.end());
    data.cross_street_names.insert(data.cross_street_names.end(), thread_data[tid].cross_street_names.begin(), thread_data[tid].cross_street_names.end());
    data.off_street_names.insert(data.off_street_names.end(), thread_data[tid].off_street_names.begin(), thread_data[tid].off_street_names.end());
    data.persons_injured.insert(data.persons_injured.end(), thread_data[tid].persons_injured.begin(), thread_data[tid].persons_injured.end());
    data.persons_killed.insert(data.persons_killed.end(), thread_data[tid].persons_killed.begin(), thread_data[tid].persons_killed.end());
    data.pedestrians_injured.insert(data.pedestrians_injured.end(), thread_data[tid].pedestrians_injured.begin(), thread_data[tid].pedestrians_injured.end());
    data.pedestrians_killed.insert(data.pedestrians_killed.end(), thread_data[tid].pedestrians_killed.begin(), thread_data[tid].pedestrians_killed.end());
    data.cyclists_injured.insert(data.cyclists_injured.end(), thread_data[tid].cyclists_injured.begin(), thread_data[tid].cyclists_injured.end());
    data.cyclists_killed.insert(data.cyclists_killed.end(), thread_data[tid].cyclists_killed.begin(), thread_data[tid].cyclists_killed.end());
    data.motorists_injured.insert(data.motorists_injured.end(), thread_data[tid].motorists_injured.begin(), thread_data[tid].motorists_injured.end());
    data.motorists_killed.insert(data.motorists_killed.end(), thread_data[tid].motorists_killed.begin(), thread_data[tid].motorists_killed.end());
    data.contributing_factor_vehicle_1.insert(data.contributing_factor_vehicle_1.end(), thread_data[tid].contributing_factor_vehicle_1.begin(), thread_data[tid].contributing_factor_vehicle_1.end());
    data.contributing_factor_vehicle_2.insert(data.contributing_factor_vehicle_2.end(), thread_data[tid].contributing_factor_vehicle_2.begin(), thread_data[tid].contributing_factor_vehicle_2.end());
    data.contributing_factor_vehicle_3.insert(data.contributing_factor_vehicle_3.end(), thread_data[tid].contributing_factor_vehicle_3.begin(), thread_data[tid].contributing_factor_vehicle_3.end());
    data.contributing_factor_vehicle_4.insert(data.contributing_factor_vehicle_4.end(), thread_data[tid].contributing_factor_vehicle_4.begin(), thread_data[tid].contributing_factor_vehicle_4.end());
    data.contributing_factor_vehicle_5.insert(data.contributing_factor_vehicle_5.end(), thread_data[tid].contributing_factor_vehicle_5.begin(), thread_data[tid].contributing_factor_vehicle_5.end());
    data.collision_ids.insert(data.collision_ids.end(), thread_data[tid].collision_ids.begin(), thread_data[tid].collision_ids.end());
    data.vehicle_type_code_1.insert(data.vehicle_type_code_1.end(), thread_data[tid].vehicle_type_code_1.begin(), thread_data[tid].vehicle_type_code_1.end());
    data.vehicle_type_code_2.insert(data.vehicle_type_code_2.end(), thread_data[tid].vehicle_type_code_2.begin(), thread_data[tid].vehicle_type_code_2.end());
    data.vehicle_type_code_3.insert(data.vehicle_type_code_3.end(), thread_data[tid].vehicle_type_code_3.begin(), thread_data[tid].vehicle_type_code_3.end());
    data.vehicle_type_code_4.insert(data.vehicle_type_code_4.end(), thread_data[tid].vehicle_type_code_4.begin(), thread_data[tid].vehicle_type_code_4.end());
    data.vehicle_type_code_5.insert(data.vehicle_type_code_5.end(), thread_data[tid].vehicle_type_code_5.begin(), thread_data[tid].vehicle_type_code_5.end());
  }

  std::cout << "Ignored Rows = " << ignoredRows << std::endl;
  std::cout << "Number of rows successfully parsed: " << data.size() << std::endl;
  return data;
}
