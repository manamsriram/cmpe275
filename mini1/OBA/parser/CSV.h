#ifndef CSV_H
#define CSV_H

#include <string>
#include <vector>

class CSV {
public:
  std::vector<std::string> crash_dates;
  std::vector<std::string> crash_times;
  std::vector<std::string> boroughs;
  std::vector<int> zip_codes;
  std::vector<double> latitudes;
  std::vector<double> longitudes;
  std::vector<std::string> locations;
  std::vector<std::string> on_street_names;
  std::vector<std::string> cross_street_names;
  std::vector<std::string> off_street_names;
  std::vector<int> persons_injured;
  std::vector<int> persons_killed;
  std::vector<int> pedestrians_injured;
  std::vector<int> pedestrians_killed;
  std::vector<int> cyclists_injured;
  std::vector<int> cyclists_killed;
  std::vector<int> motorists_injured;
  std::vector<int> motorists_killed;
  std::vector<std::string> contributing_factor_vehicle_1;
  std::vector<std::string> contributing_factor_vehicle_2;
  std::vector<std::string> contributing_factor_vehicle_3;
  std::vector<std::string> contributing_factor_vehicle_4;
  std::vector<std::string> contributing_factor_vehicle_5;
  std::vector<int> collision_ids;
  std::vector<std::string> vehicle_type_code_1;
  std::vector<std::string> vehicle_type_code_2;
  std::vector<std::string> vehicle_type_code_3;
  std::vector<std::string> vehicle_type_code_4;
  std::vector<std::string> vehicle_type_code_5;

  size_t size() const;
};

inline double parseDouble(const std::string &field);
inline int parseInt(const std::string &field);
inline std::string parseString(const std::string &field);
CSV makeCSV(const std::string &filename);

#endif