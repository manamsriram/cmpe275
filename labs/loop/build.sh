#!/bin/sh
#
# builds the loop code using different compilers
#
# Note you may have to update the versions of the compiler to match 
# your installation.

# non-threaded (gcc)
echo "building non-threaded version"
g++-13 -std=c++11 -L/usr/local/lib  loop.cpp -o loop-plain

# gcc - threaded
echo "building GNU version"
g++-13 -std=c++11 -fopenmp -L/usr/local/lib  loop.cpp -o loop-gcc

# clang - threaded
echo "building clang version"
clang++ -std=c++11 -fopenmp -L/usr/local/lib  loop.cpp -o loop-clang
