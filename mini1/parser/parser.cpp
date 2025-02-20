#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

class CSVRow {
public:
  double latitude;
  double longitude;
  int zip_code;
  int number_of_persons_injured;
  int number_of_persons_killed;
  int number_of_pedestrians_injured;
  int number_of_pedestrians_killed;
  int number_of_cyclists_injured;
  int number_of_cyclists_killed;
  int number_of_motorists_injured;
  int number_of_motorists_killed;
  int collision_id;
  std::string crash_date;
  std::string crash_time;
  std::string borough;
  std::string location;
  std::string on_street_name;
  std::string cross_street_name;
  std::string off_street_name;
  std::string contributing_factor_vehicle_1;
  std::string contributing_factor_vehicle_2;
  std::string contributing_factor_vehicle_3;
  std::string contributing_factor_vehicle_4;
  std::string contributing_factor_vehicle_5;
  std::string vehicle_type_code_1;
  std::string vehicle_type_code_2;
  std::string vehicle_type_code_3;
  std::string vehicle_type_code_4;
  std::string vehicle_type_code_5;

  void printRow() const {
    std::cout << "Latitude: " << latitude << std::endl;
    std::cout << "Longitude: " << longitude << std::endl;
    std::cout << "Crash Date: " << crash_date << std::endl;
    std::cout << "Crash Time: " << crash_time << std::endl;
    std::cout << "Zip Code: " << zip_code << std::endl;
    std::cout << "Number of Persons Injured: " << number_of_persons_injured << std::endl;
    std::cout << "Number of Persons Killed: " << number_of_persons_killed << std::endl;
    std::cout << "Number of Pedestrians Injured: " << number_of_pedestrians_injured << std::endl;
    std::cout << "Number of Pedestrians Killed: " << number_of_pedestrians_killed << std::endl;
    std::cout << "Number of Cyclists Injured: " << number_of_cyclists_injured << std::endl;
    std::cout << "Number of Cyclists Killed: " << number_of_cyclists_killed << std::endl;
    std::cout << "Number of Motorists Injured: " << number_of_motorists_injured << std::endl;
    std::cout << "Number of Motorists Killed: " << number_of_motorists_killed << std::endl;
    std::cout << "Collision ID: " << collision_id << std::endl;
    std::cout << "Borough: " << borough << std::endl;
    std::cout << "Location: " << location << std::endl;
    std::cout << "On Street Name: " << on_street_name << std::endl;
    std::cout << "Cross Street Name: " << cross_street_name << std::endl;
    std::cout << "Off Street Name: " << off_street_name << std::endl;
    std::cout << "Contributing Factor Vehicle 1: " << contributing_factor_vehicle_1 << std::endl;
    std::cout << "Contributing Factor Vehicle 2: " << contributing_factor_vehicle_2 << std::endl;
    std::cout << "Contributing Factor Vehicle 3: " << contributing_factor_vehicle_3 << std::endl;
    std::cout << "Contributing Factor Vehicle 4: " << contributing_factor_vehicle_4 << std::endl;
    std::cout << "Contributing Factor Vehicle 5: " << contributing_factor_vehicle_5 << std::endl;
    std::cout << "Vehicle Type Code 1: " << vehicle_type_code_1 << std::endl;
    std::cout << "Vehicle Type Code 2: " << vehicle_type_code_2 << std::endl;
    std::cout << "Vehicle Type Code 3: " << vehicle_type_code_3 << std::endl;
    std::cout << "Vehicle Type Code 4: " << vehicle_type_code_4 << std::endl;
    std::cout << "Vehicle Type Code 5: " << vehicle_type_code_5 << std::endl;
    std::cout << std::endl;
  }
};

class CSV {
public:
  std::vector<CSVRow> rows;

  void addRow(const CSVRow &row) { rows.push_back(row); }

  const CSVRow &getRow(size_t index) const { return rows.at(index); }

  size_t rowCount() const { return rows.size(); }

  void printHead(size_t lines = 5) const {
    for (size_t i = 0; i < lines && i < rows.size(); ++i) {
      rows[i].printRow();
    }
  }

  void printRow(size_t index) const {
    if (index < rows.size()) {
      rows[index].printRow();
    } else {
      std::cerr << "Index out of range" << std::endl;
    }
  }
};

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

// test code
// comment out when integrating
int main() {
  // std::string filename = "./Motor_Vehicle_Collisions_-_Crashes_20250210.csv";
  std::string filename = "./test.csv";
  CSV csv = makeCSV(filename);
  // std::cout << "Number of rows successfully parsed: " << csv.rowCount()
  //           << std::endl;
  // csv.printHead();
  csv.printRow(959);
  return 0;
}