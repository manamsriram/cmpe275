#ifndef SPATIAL_ANALYSIS_H
#define SPATIAL_ANALYSIS_H

#include "./parser/CSV.h"
#include <map>
#include <string>
#include <vector>

class SpatialAnalysis {
public:
  SpatialAnalysis(int injuryThreshold, int deathThreshold);
  void processCollisions(const CSV &data);
  void identifyHighRiskAreas() const;

private:
  struct YearlyStats {
    int year;
    int collisionCount;
    int injuryCount;
    int deathCount;
  };

  struct AreaStats {
    std::vector<YearlyStats> yearlyStats;
  };

  struct RiskAssessment {
    bool isHighRisk;
    bool hasReducedRisk;
  };

  std::map<std::string, std::map<int, AreaStats>> boroughZipStats;
  const int INJURY_THRESHOLD;
  const int DEATH_THRESHOLD;

  int extractYear(const std::string &date) const;
  RiskAssessment assessRisk(const std::vector<YearlyStats> &stats) const;
};

#endif