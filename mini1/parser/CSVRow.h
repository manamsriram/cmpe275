#ifndef CSVROW_H
#define CSVROW_H

#include <string>
#include <iostream>

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

  void printRow() const;
};

#endif
