#include<stdio.h>

#include "omp.h"

//#define VERBOSE
#define SHOWRESULTS

static int N = 1000000;

int main() {
    //int nthreads = 10;
    int temp = 1; 
    int a[N+1], b[N+1];

    // initialize 
    for (int i = 0; i < N; i++) {
        a[i] = temp;
        b[i] = i;
    }

#ifdef VERBOSE
    printf(" T,  i)   A   B   tmp\n");
#endif

    int tcnt = 0; 
    #pragma omp parallel
    {
        int cnt = 0;

#if defined(_OPENMP)
        int id = omp_get_thread_num();
#else
        int id = -1;
#endif

#ifdef VERBOSE
        // verify random thread ordering 
        printf("T %2d\n",id);
#endif

        //#pragma omp for schedule(static)
        //#pragma omp for schedule(dynamic)
        #pragma omp for schedule(static,10)
        for (int i = 0; i < N; i++) {
            a[i] = temp;
            a[i+1] = b[i+1];
            temp = a[i];

            // on a single socket we see more collisions
            if ( temp != 1 ) {
                cnt++;
#ifdef VERBOSE
                printf("%2d, %3d) %3d %3d %3d\n", id,i,a[i],b[i],temp);
#endif
            }
        }
 
        #pragma omp critical
        {
#ifdef SHOWRESULTS
           printf("T%02d collisions:   %5d\n", id,cnt);
#endif
           tcnt += cnt;
        }
    }

    printf("\nCollisions: %2d\n",tcnt);
}
