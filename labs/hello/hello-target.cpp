#include <iostream>

#include "omp.h"

using namespace std;

/**
 * @brief basic starting point - what should you see?
 *
 *      Author: gash
 */
int main(int argc, char **argv) {

#pragma omp parallel
  {
#pragma omp single
    {
      cout << "num threads = " << omp_get_num_threads() << endl;
      cout << "max threads = " << omp_get_max_threads() << endl;
      cout << "num proces  = " << omp_get_num_procs() << endl;
      cout << endl;
    }
  }

  int I = omp_get_num_threads();
#pragma omp target teams distribute parallel for default(none) shared(I)
  for (int i = 0; i < I; i++) {
    // cout << "Hello (" << i << ")" << endl;
    i = i * 2 + 1;
    // cout << "Hello (" << i << ")\n";
  }
}
