cmake_minimum_required(VERSION 3.22)

project (mqexample)

if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 12.0)
   # good
else ()
   message(FATAL_ERROR "Requires gcc 12.0 or greater")
endif()

find_package(Boost 1.55 REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR})

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# hold over from another build - shows how to check for libraries
#find_package(Rabbitmqc REQUIRED)
#include_directories(SYSTEM ${Rabbitmqc_INCLUDE_DIRS})
#find_package(SimpleAmqpClient REQUIRED)
#include_directories(SYSTEM ${SimpleAmqpClient_INCLUDE_DIRS})

include_directories(BEFORE src ${CMAKE_CURRENT_BINARY_DIR})

include(CheckIncludeFile)

add_subdirectory(src)
add_subdirectory(test)
