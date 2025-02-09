# NOTE: if you have subdirectories that create a library

# src files. For inlined headers, include header as a source
#set(lib-srcs
#        stuff/aclass.cpp
#)

# library targets
#add_library(stuff ${lib-srcs})
#TARGET_LINK_LIBRARIES(stuff ${Boost_LIBRARIES} ${SOCKET_LIBRARY})

# dependencies
#set(BUILD_LIBRARIES stuff ${Boost_LIBRARIES} ${SOCKET_LIBRARY} pthread)

# openmp flags
SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )


# app targets
add_executable(pi pi.cpp)
add_executable(mppi mp-pi.cpp)
add_executable(mppi2 mp-pi2.cpp)
add_executable(mppi3 mp-pi3.cpp)
add_executable(mppi4 mp-pi4.cpp)



# NOTE: these are the libraries we would have built
#target_link_libraries(hello ${BUILD_LIBRARIES})

