#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>


#include "./parser/CSV.h"
#include "SpatialAnalysis.h"

int main() {
  std::string filename = "./parser/collision_data.csv";

  CSV csv = makeCSV(filename);

  SpatialAnalysis analysis(100, 10);
  analysis.processCollisions(csv);
  analysis.identifyHighRiskAreas();

  return 0;
}