#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

class CSVRow {
public:
  double latitude;
  double longitude;
  long crash_date;
  int crash_time;
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
    std::cout
        << "Latitude: " << latitude << ", Longitude: " << longitude
        << ", Crash Date: " << crash_date << ", Crash Time: " << crash_time
        << ", Zip Code: " << zip_code
        << ", Number of Persons Injured: " << number_of_persons_injured
        << ", Number of Persons Killed: " << number_of_persons_killed
        << ", Number of Pedestrians Injured: " << number_of_pedestrians_injured
        << ", Number of Pedestrians Killed: " << number_of_pedestrians_killed
        << ", Number of Cyclists Injured: " << number_of_cyclists_injured
        << ", Number of Cyclists Killed: " << number_of_cyclists_killed
        << ", Number of Motorists Injured: " << number_of_motorists_injured
        << ", Number of Motorists Killed: " << number_of_motorists_killed
        << ", Collision ID: " << collision_id << ", Borough: " << borough
        << ", Location: " << location << ", On Street Name: " << on_street_name
        << ", Cross Street Name: " << cross_street_name
        << ", Off Street Name: " << off_street_name
        << ", Contributing Factor Vehicle 1: " << contributing_factor_vehicle_1
        << ", Contributing Factor Vehicle 2: " << contributing_factor_vehicle_2
        << ", Contributing Factor Vehicle 3: " << contributing_factor_vehicle_3
        << ", Contributing Factor Vehicle 4: " << contributing_factor_vehicle_4
        << ", Contributing Factor Vehicle 5: " << contributing_factor_vehicle_5
        << ", Vehicle Type Code 1: " << vehicle_type_code_1
        << ", Vehicle Type Code 2: " << vehicle_type_code_2
        << ", Vehicle Type Code 3: " << vehicle_type_code_3
        << ", Vehicle Type Code 4: " << vehicle_type_code_4
        << ", Vehicle Type Code 5: " << vehicle_type_code_5 << std::endl;
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
};

CSV makeCSV(std::string filename) {
  CSV csv;
  std::ifstream file(filename);
  std::string line;

  while (std::getline(file, line)) {
    std::istringstream s(line);
    std::string field;
    CSVRow row;

    std::getline(s, field, ',');
    row.latitude = std::stod(field);
    std::getline(s, field, ',');
    row.longitude = std::stod(field);
    std::getline(s, field, ',');
    row.crash_date = std::stol(field);
    std::getline(s, field, ',');
    row.crash_time = std::stoi(field);
    std::getline(s, field, ',');
    row.zip_code = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_persons_injured = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_persons_killed = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_pedestrians_injured = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_pedestrians_killed = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_cyclists_injured = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_cyclists_killed = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_motorists_injured = std::stoi(field);
    std::getline(s, field, ',');
    row.number_of_motorists_killed = std::stoi(field);
    std::getline(s, field, ',');
    row.collision_id = std::stoi(field);
    std::getline(s, field, ',');
    row.borough = field;
    std::getline(s, field, ',');
    row.location = field;
    std::getline(s, field, ',');
    row.on_street_name = field;
    std::getline(s, field, ',');
    row.cross_street_name = field;
    std::getline(s, field, ',');
    row.off_street_name = field;
    std::getline(s, field, ',');
    row.contributing_factor_vehicle_1 = field;
    std::getline(s, field, ',');
    row.contributing_factor_vehicle_2 = field;
    std::getline(s, field, ',');
    row.contributing_factor_vehicle_3 = field;
    std::getline(s, field, ',');
    row.contributing_factor_vehicle_4 = field;
    std::getline(s, field, ',');
    row.contributing_factor_vehicle_5 = field;
    std::getline(s, field, ',');
    row.vehicle_type_code_1 = field;
    std::getline(s, field, ',');
    row.vehicle_type_code_2 = field;
    std::getline(s, field, ',');
    row.vehicle_type_code_3 = field;
    std::getline(s, field, ',');
    row.vehicle_type_code_4 = field;
    std::getline(s, field, ',');
    row.vehicle_type_code_5 = field;

    csv.addRow(row);
  }

  return csv;
}