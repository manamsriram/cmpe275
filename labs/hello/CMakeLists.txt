cmake_minimum_required(VERSION 3.3)

project (hello)

# compilers to use (9.2)
set(CMAKE_C_COMPILER /opt/homebrew/opt/llvm/bin/clang)
set(CMAKE_CXX_COMPILER  /opt/homebrew/opt/llvm/bin/clang++)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# openmp flags
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp --target=x86_64-pc-linux-gnu")
SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )


# app targets
add_executable(hello hello-plain.cpp)
add_executable(hello-omp hello-omp.cpp)
add_executable(hello-cout hello-cout.cpp)

# advanced example
add_executable(hello-target hello-target.cpp)


