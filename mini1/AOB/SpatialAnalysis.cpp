#include "./parser/CSV.h"
#include "SpatialAnalysis.h"
#include <iostream>
#include <algorithm>
#include <vector>
#include <sstream>
#include <omp.h>

SpatialAnalysis::SpatialAnalysis(int injuryThreshold, int deathThreshold)
    : INJURY_THRESHOLD(injuryThreshold), DEATH_THRESHOLD(deathThreshold) {}

void SpatialAnalysis::processCollisions(const std::vector<CSVRow>& data) {
    #pragma omp parallel for
    for (const auto& row : data) {
        if (!row.borough.empty() && row.zip_code > 0) {
            int year = extractYear(row.crash_date);
            #pragma omp critical
            {
            auto& areaStats = boroughZipStats[row.borough][row.zip_code];
            
            auto it = std::lower_bound(areaStats.yearlyStats.begin(), areaStats.yearlyStats.end(), year,
                [](const YearlyStats& stats, int y) { return stats.year < y; });
            
            if (it == areaStats.yearlyStats.end() || it->year != year) {
                it = areaStats.yearlyStats.insert(it, {year, 0, 0, 0});
            }
            
            it->collisionCount++;
            it->injuryCount += std::max(0, row.persons_injured);
            it->deathCount += std::max(0, row.persons_killed);
            }
        }
    }
}
    
    
void SpatialAnalysis::identifyHighRiskAreas() const {
    std::cout << "Risk assessment by borough and zip code:\n";
    std::vector<std::pair<std::string, const std::map<int, AreaStats>&>> borough_pairs(boroughZipStats.begin(), boroughZipStats.end());

    #pragma omp parallel
    {
        std::ostringstream thread_output;
        #pragma omp for schedule(dynamic)
        for (size_t i = 0; i < borough_pairs.size(); ++i) {
            const auto& borough = borough_pairs[i].first;
            const auto& zipStats = borough_pairs[i].second;
            for (const auto& [zipCode, areaStats] : zipStats) {
                auto assessment = assessRisk(areaStats.yearlyStats);
                if (assessment.isHighRisk) {
                    thread_output << "Borough: " << borough << ", Zip Code: " << zipCode << " ";
                    if (assessment.hasReducedRisk) {
                        thread_output << "(REDUCED RISK)\n";
                    } else {
                        thread_output << "(HIGH RISK)\n";
                    }
                    for (const auto& yearStats : areaStats.yearlyStats) {
                        thread_output << "  Year " << yearStats.year << ": "
                                      << yearStats.collisionCount << " collisions, "
                                      << yearStats.injuryCount << " injuries, "
                                      << yearStats.deathCount << " deaths\n";
                    }
                    thread_output << std::endl;
                }
            }
        }
        #pragma omp critical
        {
            std::cout << thread_output.str();
        }
    }
}



int SpatialAnalysis::extractYear(const std::string& date) const {
    return std::stoi(date.substr(6, 4));
}

SpatialAnalysis::RiskAssessment SpatialAnalysis::assessRisk(const std::vector<YearlyStats>& stats) const {
    if (stats.empty()) return {false, false};

    bool everHighRisk = false;
    bool hasReducedRisk = false;
    int lastHighRiskYear = -1;

    for (size_t i = 0; i < stats.size(); ++i) {
        const auto& yearStat = stats[i];
        bool currentYearHighRisk = (yearStat.injuryCount >= INJURY_THRESHOLD || yearStat.deathCount >= DEATH_THRESHOLD);

        if (currentYearHighRisk) {
            everHighRisk = true;
            lastHighRiskYear = yearStat.year;
        } else if (everHighRisk && i > 0) {
            const auto& lastHighRiskStat = *std::find_if(stats.rbegin(), stats.rend(),
                [lastHighRiskYear](const YearlyStats& s) { return s.year == lastHighRiskYear; });
            
            if (yearStat.injuryCount < lastHighRiskStat.injuryCount && yearStat.deathCount < lastHighRiskStat.deathCount) {
                hasReducedRisk = true;
            }
        }
    }

    return {everHighRisk, hasReducedRisk};
}
