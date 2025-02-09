
## Compile and run:

This example requires that you have installed gcc/g++ (or clang), and cmake.

## Compiling with cmake:

...
   [prompt]: mkdir build 
   [prompt]: cd build 
   [prompt]: cmake .. 
   [prompt]: make
...

## Compiling by commandline:

...
   [prompt]: g++ -O3 -fopenmp mppi3.cpp -o mppi3
   [prompt]: time ./mppi3
   [prompt]: time OMP_NUM_THREADS=2 ./mppi3
...


Note this code will need to be converted to a C++ class


