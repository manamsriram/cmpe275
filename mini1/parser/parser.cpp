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
  std::getline(file, line); // Skip the header line

  while (std::getline(file, line)) {
    std::istringstream s(line);
    CSVRow row;
    std::string field;

    auto parseField = [&s, &field](auto& value, auto&& converter) {
      std::getline(s, field, ',');
      if (!field.empty()) {
        try {
          value = converter(field);
        } catch (const std::exception& e) {
          value = {};
        }
      } else {
        value = {};
      }
    };

    parseField(row.latitude, [](const std::string& s) { return std::stod(s); });
    parseField(row.longitude, [](const std::string& s) { return std::stod(s); });
    parseField(row.zip_code, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_persons_injured, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_persons_killed, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_pedestrians_injured, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_pedestrians_killed, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_cyclists_injured, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_cyclists_killed, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_motorists_injured, [](const std::string& s) { return std::stoi(s); });
    parseField(row.number_of_motorists_killed, [](const std::string& s) { return std::stoi(s); });
    parseField(row.collision_id, [](const std::string& s) { return std::stoi(s); });

    std::getline(s, row.crash_date, ',');
    std::getline(s, row.crash_time, ',');
    std::getline(s, row.borough, ',');
    std::getline(s, row.location, ',');
    std::getline(s, row.on_street_name, ',');
    std::getline(s, row.cross_street_name, ',');
    std::getline(s, row.off_street_name, ',');
    std::getline(s, row.contributing_factor_vehicle_1, ',');
    std::getline(s, row.contributing_factor_vehicle_2, ',');
    std::getline(s, row.contributing_factor_vehicle_3, ',');
    std::getline(s, row.contributing_factor_vehicle_4, ',');
    std::getline(s, row.contributing_factor_vehicle_5, ',');
    std::getline(s, row.vehicle_type_code_1, ',');
    std::getline(s, row.vehicle_type_code_2, ',');
    std::getline(s, row.vehicle_type_code_3, ',');
    std::getline(s, row.vehicle_type_code_4, ',');
    std::getline(s, row.vehicle_type_code_5);

    csv.addRow(row);
  }

  return csv;
}



int main() {
  std::string filename =
      "./collision_data.csv";
  CSV csv = makeCSV(filename);

  std::cout << "Number of rows: " << csv.rowCount() << std::endl;
  csv.printHead();

  return 0;
}