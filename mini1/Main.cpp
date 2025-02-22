#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "./parser/CSVRow.h"
#include "./parser/CSV.h"
#include "SpatialAnalysis.h" 

// Helper functions to parse CSV values that may be empty or "NULL"
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

CSV makeCSV(const std::string &filename) {
  int ignoredRows = 0;
  CSV csv;
  std::ifstream file(filename);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return csv;
  }

  // Skip header line
  std::string line;
  if (!std::getline(file, line)) {
    return csv;
  }

  // Read each row until EOF
  while (std::getline(file, line)) {
    std::istringstream s(line);
    CSVRow row;
    std::string field;
    try {
      // 1: CRASH DATE
      std::getline(s, field, ',');
      row.crash_date = parseString(field);

      // 2: CRASH TIME
      std::getline(s, field, ',');
      row.crash_time = parseString(field);

      // 3: BOROUGH
      std::getline(s, field, ',');
      row.borough = parseString(field);

      // 4: ZIP CODE
      std::getline(s, field, ',');
      row.zip_code = parseInt(field);

      // 5: LATITUDE
      std::getline(s, field, ',');
      row.latitude = parseDouble(field);

      // 6: LONGITUDE
      std::getline(s, field, ',');
      row.longitude = parseDouble(field);

      // 7: LOCATION
      std::getline(s, field, ',');
      row.location = parseString(field);

      // 8: ON STREET NAME
      std::getline(s, field, ',');
      row.on_street_name = parseString(field);

      // 9: CROSS STREET NAME
      std::getline(s, field, ',');
      row.cross_street_name = parseString(field);

      // 10: OFF STREET NAME
      std::getline(s, field, ',');
      row.off_street_name = parseString(field);

      // 11: NUMBER OF PERSONS INJURED
      std::getline(s, field, ',');
      row.number_of_persons_injured = parseInt(field);

      // 12: NUMBER OF PERSONS KILLED
      std::getline(s, field, ',');
      row.number_of_persons_killed = parseInt(field);

      // 13: NUMBER OF PEDESTRIANS INJURED
      std::getline(s, field, ',');
      row.number_of_pedestrians_injured = parseInt(field);

      // 14: NUMBER OF PEDESTRIANS KILLED
      std::getline(s, field, ',');
      row.number_of_pedestrians_killed = parseInt(field);

      // 15: NUMBER OF CYCLIST INJURED
      std::getline(s, field, ',');
      row.number_of_cyclists_injured = parseInt(field);

      // 16: NUMBER OF CYCLIST KILLED
      std::getline(s, field, ',');
      row.number_of_cyclists_killed = parseInt(field);

      // 17: NUMBER OF MOTORIST INJURED
      std::getline(s, field, ',');
      row.number_of_motorists_injured = parseInt(field);

      // 18: NUMBER OF MOTORIST KILLED
      std::getline(s, field, ',');
      row.number_of_motorists_killed = parseInt(field);

      // 19: CONTRIBUTING FACTOR VEHICLE 1
      std::getline(s, field, ',');
      row.contributing_factor_vehicle_1 = parseString(field);

      // 20: CONTRIBUTING FACTOR VEHICLE 2
      std::getline(s, field, ',');
      row.contributing_factor_vehicle_2 = parseString(field);

      // 21: CONTRIBUTING FACTOR VEHICLE 3
      std::getline(s, field, ',');
      row.contributing_factor_vehicle_3 = parseString(field);

      // 22: CONTRIBUTING FACTOR VEHICLE 4
      std::getline(s, field, ',');
      row.contributing_factor_vehicle_4 = parseString(field);

      // 23: CONTRIBUTING FACTOR VEHICLE 5
      std::getline(s, field, ',');
      row.contributing_factor_vehicle_5 = parseString(field);

      // 24: COLLISION_ID
      std::getline(s, field, ',');
      row.collision_id = parseInt(field);

      // 25: VEHICLE TYPE CODE 1
      std::getline(s, field, ',');
      row.vehicle_type_code_1 = parseString(field);

      // 26: VEHICLE TYPE CODE 2
      std::getline(s, field, ',');
      row.vehicle_type_code_2 = parseString(field);

      // 27: VEHICLE TYPE CODE 3
      std::getline(s, field, ',');
      row.vehicle_type_code_3 = parseString(field);

      // 28: VEHICLE TYPE CODE 4
      std::getline(s, field, ',');
      row.vehicle_type_code_4 = parseString(field);

      // 29: VEHICLE TYPE CODE 5
      std::getline(s, field, ',');
      row.vehicle_type_code_5 = parseString(field);

      csv.addRow(row);
    } catch (const std::exception &e) {
      ignoredRows++;
    }
  }
  std::cout << "Ignored Rows = " << ignoredRows << std::endl;
  std::cout << "Number of rows successfully parsed: " << csv.rowCount()
            << std::endl;
  return csv;
}

int main() {
  std::string filename = "./parser/collision_data.csv";
  
  CSV csv = makeCSV(filename);

  SpatialAnalysis analysis(100, 10);
    analysis.processCollisions(csv);
    analysis.identifyHighRiskAreas();

  return 0;
}
