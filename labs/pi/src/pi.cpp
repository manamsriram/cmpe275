#include <iostream>

#include "omp.h"

static long num_steps = 5000000; 
double step; 

int main(int argc, char **argv) {

   int i; double x, pi, sum = 0.0; 
 
   step = 1.0/(double) num_steps; 
 
   //#pragma omp parallel
   for ( i=0; i< num_steps; i++ ) { 
      x = (i+0.5)*step; 
      sum = sum + 4.0/(1.0+x*x); 
   } 

   pi = step * sum; 

  printf("pi = %f\n",pi);
} 
