# Introduction to threads in C++

Seems that every language introductory tutorial starts with a program to print *hello world*, 
and yes, (◔_◔), here it is. For some (many), this can very well be your first program using 
C/C++. Though the focus of this lab is really on parallel programming, specifically threading
using a package called OpenMP. Their are other threading packages available. For instance,
POSIX pthreads, Thrust (https://github.com/NVIDIA/thrust), and TBB (https://github.com/oneapi-src/oneTBB)

This is our first introduction to parallel coding using directive-based coding. That is we
are not going to use direct calls to phtreads or other thread packages rather, in the case of
OpenMP, we use pre-processor directives that are substituted at compile time into native
threading code. 

This project uses Cmake (https://cmake.org) a make tool available through many OS installers 
(apt-get, yum, brew). So, in addition to installing gnu or clang, you will need to install 
Cmake.

So let's get started. 

## Tutorials

  - https://www.geeksforgeeks.org/packaged-task-advanced-c-multithreading-multiprocessing/?ref=rp
  - https://www.openmp.org/resources/tutorials-articles/
  - See lecture references/notes

## Requirements:

  - gcc/g++ 11.x or newer
  - CMake


## Compiling:

...
   [prompt]: mkdir build
   [prompt]: cd build
   [prompt]: cmake ..
...


## Running:

   * Look at the code and see what you think will be outputted to the screen.

   * Run the code

...
     [prompt]: ./[exe_name]
...

   * Try changing the number of threads and run again

...
     [prompt]: OMP_NUM_THREADS=4 ./[exe_name]
...

   * Measure execute (wallclock) time using the command `time`

...
     [prompt]: time OMP_NUM_THREADS=4 ./[exe_name]
...


## Cleanup:

...
    [prompt] cd ..
    [prompt] rm -rf build
...

