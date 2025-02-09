#!/bin/bash
#
# remove cmake created files
#
# When running cmake, local cmake/make files are created, this
# script removes these files. A cvsigore file should be used
# to exclude these files/dirs from the code repository.
#

# define the base source directory relative to this script
SRCDIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# don't let this script run anywhere except this directory
base=`basename $SRCDIR`
if [ $base == 'basic' ]; then
   echo -e "\nDeleting build aritfacts in $SRCDIR in 3 seconds...\n"
   sleep 3
else
    echo -e "\nInvalid directory, exitting"
    exit
fi

find ${SRCDIR} -name "cmake_install.cmake" -exec rm {} \;
find ${SRCDIR} -name "CMakeCache.txt" -exec rm {} \;
find ${SRCDIR} -name "Makefile" -exec rm {} \;
find ${SRCDIR} -name "CMakeFiles" -exec rm -r {} \;
find ${SRCDIR} -name "CmakeLists.txt" -exec rm {} \;
find ${SRCDIR}/src -name "*.cmake" -exec rm {} \;
find ${SRCDIR}/src -name "*.a" -exec rm {} \;
find ${SRCDIR}/src -name "*.o" -exec rm {} \;
find ${SRCDIR}/test -name "*.cmake" -exec rm {} \;

echo -e "\nAny build directories will be left in an inconsistent state.\n"

# You don't want to run this as we have .cmake files in 
# Modules that are needed to create the makefiles
#find ${SRCDIR} -name "*.cmake" -exec rm {} \;

