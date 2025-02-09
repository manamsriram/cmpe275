#include <iostream>

#include "omp.h"

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
         std::cout << "num threads = " << omp_get_num_threads() << std::endl;
         std::cout << "max threads = " << omp_get_max_threads() << std::endl;
         std::cout << "num proces  = " << omp_get_num_procs() << std::endl;
         std::cout << std::endl;
      }

   }

   int I = omp_get_max_threads();
   #pragma omp parallel for
   for ( int i = 0 ; i < I ; i++) {
      std::cout << "Hello (" << i << ")" << std::endl;
   }

}
