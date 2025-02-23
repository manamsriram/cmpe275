cmake_minimum_required(VERSION 3.3)

project (mini1)

# compilers to use (9.2)
set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/14.2.0_1/bin/gcc-14)
set(CMAKE_CXX_COMPILER /opt/homebrew/Cellar/gcc/14.2.0_1/bin/g++-14)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# openmp flags
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp --target=x86_64-pc-linux-gnu")
SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )


# app targets
add_executable(main1 ./parser/CSV.cpp SpatialAnalysis.cpp main.cpp)