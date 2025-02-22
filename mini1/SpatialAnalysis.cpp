#include "SpatialAnalysis.h"
#include <iostream>
#include <algorithm>

SpatialAnalysis::SpatialAnalysis(int injuryThreshold, int deathThreshold)
    : INJURY_THRESHOLD(injuryThreshold), DEATH_THRESHOLD(deathThreshold) {}

void SpatialAnalysis::processCollisions(const CSV& csv) {
    for (const auto& row : csv.rows) {
        if (!row.borough.empty() && row.zip_code > 0) {
            int year = extractYear(row.crash_date);
            auto& areaStats = boroughZipStats[row.borough][row.zip_code];
            
            auto it = std::find_if(areaStats.yearlyStats.begin(), areaStats.yearlyStats.end(),
                [year](const YearlyStats& stats) { return stats.year == year; });
            
            if (it == areaStats.yearlyStats.end()) {
                areaStats.yearlyStats.push_back({year, 1, row.number_of_persons_injured, row.number_of_persons_killed});
            } else {
                it->collisionCount++;
                it->injuryCount += row.number_of_persons_injured;
                it->deathCount += row.number_of_persons_killed;
            }
        }
    }
}

void SpatialAnalysis::identifyHighRiskAreas() const {
    std::cout << "Risk assessment by borough and zip code:\n";
    for (const auto& [borough, zipStats] : boroughZipStats) {
        for (const auto& [zipCode, areaStats] : zipStats) {
            auto assessment = assessRisk(areaStats.yearlyStats);
            if (assessment.isHighRisk) {
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
