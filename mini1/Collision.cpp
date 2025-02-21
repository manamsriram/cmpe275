#include "Collision.h"
#include <iostream>
#include <algorithm>

Collision::Collision(const CSV& csvData) : csv(csvData), boroughs({"MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"}) {}

void Collision::performAnalysis() {
    for (const auto& row : csv.rows) {
        //std::cout << row.borough << " " << row.zip_code << std::endl;
        if (row.borough.empty() || row.zip_code <= 0) continue;
        boroughData[row.borough][row.zip_code]++;
    }

    for (const auto& [borough, zipData] : boroughData) {
        std::vector<std::pair<int, int>> zipCounts(zipData.begin(), zipData.end());
        std::sort(zipCounts.begin(), zipCounts.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });
        sortedBoroughData[borough] = zipCounts;
    }
}

void Collision::displayResults() {
    // Debug: Print contents of sortedBoroughData
    std::cout << "sortedBoroughData contains the following boroughs:\n";
    for (const auto& [borough, zipData] : sortedBoroughData) {
        std::cout << "  \"" << borough << "\"\n"; 
    }
    std::cout << std::endl;

    // Debug: Print contents of boroughs vector
    std::cout << "boroughs vector contains:\n";
    for (const auto& borough : boroughs) {
        std::cout << "  \"" << borough << "\"\n"; 
    }
    std::cout << std::endl;


    for (const auto& borough : boroughs) {
        if (sortedBoroughData.find(borough) != sortedBoroughData.end()) {
            std::cout << "Borough: " << borough << std::endl;
            std::cout << "Zip code counts:" << std::endl;
            for (const auto& [zipCode, count] : sortedBoroughData[borough]) {
                std::cout << "  " << zipCode << ": " << count << std::endl;
            }
            std::cout << std::endl;
        }
    }
}
