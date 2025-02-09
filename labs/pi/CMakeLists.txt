cmake_minimum_required(VERSION 2.8)

project (pithreading)


# compilers to use (> 9.2)
set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/12.2.0/bin/gcc-12)
set(CMAKE_CXX_COMPILER  /opt/homebrew/Cellar/gcc/12.2.0/bin/g++-12)
 
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# openmp flags
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp --target=x86_64-pc-linux-gnu")
SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )

#find_package(Boost 1.55 REQUIRED)
#include_directories(${Boost_INCLUDE_DIRS})
#include_directories(${CMAKE_SOURCE_DIR})

#set(Boost_USE_STATIC_LIBS ON)
#set(Boost_USE_MULTITHREADED ON)
#set(Boost_USE_STATIC_RUNTIME ON)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

include_directories(BEFORE src ${CMAKE_CURRENT_BINARY_DIR})

include(CheckIncludeFile)

add_subdirectory(src)
#add_subdirectory(test)
