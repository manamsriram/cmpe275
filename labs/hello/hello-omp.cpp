#include <iostream>
#include <stdio.h>

#include "omp.h"

/**
 * @brief basic starting point - adding openMP calls
 *
 *      Author: gash
 */
int main(int argc, char **argv) {

   int numD = omp_get_num_devices();
   int numT = omp_get_num_threads();
   int maxT = omp_get_max_threads();
   int maxP = omp_get_num_procs();

   printf("\nnum threads: %d\n",numT);
   printf("max threads: %d\n",maxT);
   printf("num devices: %d\n",numD);
   printf("num procs  : %d\n\n",maxP);
     
   #pragma omp parallel
   {
     int ID = omp_get_thread_num();
     printf("Hello (%d)\n",ID);
   }

}
