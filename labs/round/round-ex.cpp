#include <iostream>
#include <iomanip>
#include <cmath>

#if defined(__x86_64__)
#include <boost/chrono.hpp>
#endif

#include "omp.h"

#if defined(__x86_64__)
using namespace boost::chrono;
#endif

/**
 * @brief basic boost chrono
 *
 * build using cmake, or to build by hand (note location and 
 * version of boost is installation dependent):

 * g++-11 -I /usr/local/gcc/boost_1_60_0/include -L /usr/local/gcc/boost_1_60_0/lib -lboost_chrono -lboost_system -std=c++14 round-ex.cpp -o round-ex
 *
 *      Author: gash
 */
int main(int argc, char **argv) {

    const auto A = 2048;
    const auto B = 1024;

    // Known answer: reference results (1 thread)
    auto ref = 0.0f;

    for ( int a = 1 ; a < A ; a++ ) {
        for ( int b = 1 ; b < B ; b++ ) {
            ref += cos(a) / sqrt(b);
        }
    }

#if defined(__x86_64__)
    // conflict for M1 and boost chrono lib
    auto dt_s = high_resolution_clock::now();
#endif

    std::cout << "\nref = " << std::setprecision(15) << ref << "\n";
    std::cout << "----------------------------------------------------\n";
    for ( int n = 0 ; n < 10 ; n++ ) {
       auto val = 0.0f;
       #pragma omp parallel
       {
           #pragma omp for schedule(guided) reduction(+:val)
           for ( int a = 1 ; a < A ; a++ ) {
               for ( int b = 1 ; b < B ; b++ ) {
                   val += cos(a) / sqrt(b);
               }
           } // implied join (mutex)
       }
 
#if defined(__x86_64__)
       auto dt = duration_cast<nanoseconds> (high_resolution_clock::now() - dt_s);
#else
       auto dt = 0.0f;
#endif

#if defined(__x86_64__)
       std::cout << n <<  ") " << std::setprecision(15) << val << " (dt = " << dt.count() << " ns), \t";
#else
       std::cout << n  << ") " << std::setprecision(15) << val << ", \t";
#endif

       std::cout << "agreement: " << std::setprecision(2) << (ref - val) << "\n";
   }

}
