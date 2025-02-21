#ifndef COLLISION_H
#define COLLISION_H

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include "./parser/CSV.h" 

class Collision {
public:
    Collision(const CSV& csvData);
    void performAnalysis();
    void displayResults();

private:
    const CSV& csv;
    std::map<std::string, std::map<int, int>> boroughData;
    std::map<std::string, std::vector<std::pair<int, int>>> sortedBoroughData;
    std::vector<std::string> boroughs; 
    
};

#endif
