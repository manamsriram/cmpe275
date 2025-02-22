#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>


int main() {
  std::vector<double> times;
  // number of tests to run
  times.reserve(10);

  for (int i = 0; i < 10; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    // name of file to run

    system(".\\main1.exe");
    // comment out the above line and uncomment the one below for mac and linux systems
    // system("./main1");
    auto end = std::chrono::high_resolution_clock::now();

    double elapsed = std::chrono::duration<double>(end - start).count();
    times.push_back(elapsed);
  }


  // printing the results out
  std::cout << std::setw(8) << "Run #" << " | " << "Time (s)\n";
  std::cout << "----------------------\n";
  for (int i = 0; i < 10; i++) {
    std::cout << std::setw(5) << (i + 1) << "   | " << times[i] << "\n";
  }

  return 0;
}