#include "CSVRow.h"
#include <iostream>

void CSVRow::printRow() const {
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
