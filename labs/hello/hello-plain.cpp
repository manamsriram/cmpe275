#include <iostream>
#include "omp.h"

/**
 * @brief basic starting point - no threading
 *
 *      Author: gash
 */
int main(int argc, char **argv) {

   #pragma omp parallel
   {
     //int ID = -1;
     int ID = omp_get_thread_num();
     printf("Hello (%d)\n",ID);
   }

}
