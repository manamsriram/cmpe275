#include "SpatialAnalysis.h"
#include <algorithm>
#include <iostream>
#include <omp.h>

SpatialAnalysis::SpatialAnalysis(int injuryThreshold, int deathThreshold)
    : INJURY_THRESHOLD(injuryThreshold), DEATH_THRESHOLD(deathThreshold) {}

void SpatialAnalysis::processCollisions(const CSV &data) {
  #pragma omp parallel
  {
    std::map<std::string, std::map<int, AreaStats>> thread_local_stats;

    #pragma omp for
    for (size_t i = 0; i < data.size(); ++i) {
      if (!data.boroughs[i].empty() && data.zip_codes[i] > 0) {
        int year = extractYear(data.crash_dates[i]);
        auto &areaStats = thread_local_stats[data.boroughs[i]][data.zip_codes[i]];

        auto it = std::lower_bound(
            areaStats.yearlyStats.begin(), areaStats.yearlyStats.end(), year,
            [](const YearlyStats &stats, int y) { return stats.year < y; });

        if (it == areaStats.yearlyStats.end() || it->year != year) {
          it = areaStats.yearlyStats.insert(it, {year, 0, 0, 0});
        }

        it->collisionCount++;
        it->injuryCount += std::max(0, data.persons_injured[i]);
        it->deathCount += std::max(0, data.persons_killed[i]);
      }
    }

    #pragma omp critical
    {
      for (const auto &borough_pair : thread_local_stats) {
        for (const auto &zip_pair : borough_pair.second) {
          auto &global_stats = boroughZipStats[borough_pair.first][zip_pair.first];
          for (const auto &year_stats : zip_pair.second.yearlyStats) {
            auto it = std::lower_bound(
                global_stats.yearlyStats.begin(), global_stats.yearlyStats.end(), year_stats.year,
                [](const YearlyStats &stats, int y) { return stats.year < y; });
            if (it == global_stats.yearlyStats.end() || it->year != year_stats.year) {
              it = global_stats.yearlyStats.insert(it, year_stats);
            } else {
              it->collisionCount += year_stats.collisionCount;
              it->injuryCount += year_stats.injuryCount;
              it->deathCount += year_stats.deathCount;
            }
          }
        }
      }
    }
  }
}    
    
    

void SpatialAnalysis::identifyHighRiskAreas() const {
  std::cout << "Risk assessment by borough and zip code:\n";
  #pragma omp parallel
  {
    #pragma omp single
    {
      for (const auto& boroughPair : boroughZipStats) {
        const auto& borough = boroughPair.first;
        const auto& zipStats = boroughPair.second;
        for (const auto& zipPair : zipStats) {
          const auto& zipCode = zipPair.first;
          const auto& areaStats = zipPair.second;
          #pragma omp task
          {
            auto assessment = assessRisk(areaStats.yearlyStats);
            if (assessment.isHighRisk) {
              #pragma omp critical
              {
                std::cout << "Borough: " << borough << ", Zip Code: " << zipCode << " ";
                if (assessment.hasReducedRisk) {
                  std::cout << "(REDUCED RISK)\n";
                } else {
                  std::cout << "(HIGH RISK)\n";
                }
                for (const auto& yearStats : areaStats.yearlyStats) {
                  std::cout << "  Year " << yearStats.year << ": "
                            << yearStats.collisionCount << " collisions, "
                            << yearStats.injuryCount << " injuries, "
                            << yearStats.deathCount << " deaths\n";
                }
                std::cout << std::endl;
              }
            }
          }
        }
      }
    }
  }
}




int SpatialAnalysis::extractYear(const std::string &date) const {
  return std::stoi(date.substr(6, 4));
}

SpatialAnalysis::RiskAssessment
SpatialAnalysis::assessRisk(const std::vector<YearlyStats> &stats) const {
  if (stats.empty())
    return {false, false};

  bool everHighRisk = false;
  bool hasReducedRisk = false;
  int lastHighRiskYear = -1;

  for (size_t i = 0; i < stats.size(); ++i) {
    const auto &yearStat = stats[i];
    bool currentYearHighRisk = (yearStat.injuryCount >= INJURY_THRESHOLD ||
                                yearStat.deathCount >= DEATH_THRESHOLD);

    if (currentYearHighRisk) {
      everHighRisk = true;
      lastHighRiskYear = yearStat.year;
    } else if (everHighRisk && i > 0) {
      const auto &lastHighRiskStat =
          *std::find_if(stats.rbegin(), stats.rend(),
                        [lastHighRiskYear](const YearlyStats &s) {
                          return s.year == lastHighRiskYear;
                        });

      if (yearStat.injuryCount < lastHighRiskStat.injuryCount &&
          yearStat.deathCount < lastHighRiskStat.deathCount) {
        hasReducedRisk = true;
      }
    }
  }

  return {everHighRisk, hasReducedRisk};
}