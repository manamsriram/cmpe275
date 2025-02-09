#include <iostream>

#include "omp.h"

// how many threads to run
#define NUM_THREADS 1

static long num_steps = 5000000; 
double step; 

int main(int argc, char **argv) {

   double step = 1.0/(double) num_steps; 
   double pi = 0.0;
 
   omp_set_num_threads(NUM_THREADS);
   #pragma omp parallel
   {
      double sum = 0.0;

      // we are striding over the array
      int id = omp_get_thread_num();
      for ( int i=id; i< num_steps; i += NUM_THREADS ) { 
         double x = (i+0.5)*step; 
         sum = sum + 4.0/(1.0+x*x); 
      } 

      #pragma omp critical
      pi += step * sum; 
   }

  printf("pi = %f\n",pi);
} 
