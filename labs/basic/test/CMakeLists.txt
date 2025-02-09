# do we need this test or just let if fail?
#find_package(Boost)
#if (Boost_FOUND)
#  include_directories( ${Boost_INCLUDE_DIRS} )
#endif()

# for testing using google's test framework
enable_testing(true)

# include paths
include_directories(../src)
include_directories( ${Boost_INCLUDE_DIRS} )
include_directories(${CMAKE_SOURCE_DIR})

set(BUILD_LIBRARIES ${Boost_LIBRARIES} ${SOCKET_LIBRARY} pthread)

# define targets
add_executable(demoapp demoapp.cpp)
target_link_libraries(demoapp  ${BUILD_LIBRARIES})
