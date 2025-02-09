#include <iostream>

#include "omp.h"

// how many threads to run
#define NUM_THREADS 1

// false sharing 
#define PAD 8

static long num_steps = 5000000; 
double step; 

int main(int argc, char **argv) {

   double step = 1.0/(double) num_steps; 
 
   // we need to collect the individual values from the threads
   double sum[NUM_THREADS][PAD]; 
   for ( int i=0; i< NUM_THREADS; i++ ) 
      sum[i][0] = 0.0; 

   omp_set_num_threads(NUM_THREADS);

   #pragma omp parallel
   {
      // we are striding over the array
      int id = omp_get_thread_num();
      for ( int i=id; i< num_steps; i += NUM_THREADS ) { 
         double x = (i+0.5)*step; 
         sum[id][0] = sum[id][0] + 4.0/(1.0+x*x); 
      } 
   }

   // here we are done with thread execution
   double pi = 0.0;
   for ( int i = 0 ; i < NUM_THREADS ; i++ )
      pi += step * sum[i][0]; 

  printf("pi = %f\n",pi);
} 
