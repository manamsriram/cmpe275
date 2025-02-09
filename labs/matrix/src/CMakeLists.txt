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

# app targets
add_executable(matrix matrix.cpp)


