#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include "hashmap.h"
#ifndef N_READ_THREADS
#define N_READ_THREADS 3
#endif

HashMap* hm;

typedef struct {
    int start, end;
} st_en;

int test_fail = 0;

void* do_reads(void* arg) {
    st_en ste = *(st_en*) arg;
    int start = ste.start;
    const int end = ste.end;
    for (; start < end; start++) {
        char buf1[100];
        char* buf = buf1;
        sprintf(buf1, "-%d", start);
        const char* val1 = MapGet(hm, buf + 1);
        const char* val2 = MapGet(hm, buf);
        if (val2 != NULL) {
            fprintf(stderr, "ERROR! Map found key [%s], while it was never inserted\n", buf);
            test_fail++;
        }
        if (val1 == NULL) {
            fprintf(stderr, "ERROR! Map did NOT found key [%s]\n", buf+1);
            test_fail++;
        }
        char exp_out[100];
        sprintf(exp_out, "for key = %d, value = %d", start, start + 1);
        if (strncmp(exp_out, val1, strlen(exp_out)) != 0) {
            fprintf(stderr, "ERROR! value mismatch for key [%s]. Expected [%s], got [%s]\n", buf + 1, exp_out, val1);
            test_fail++;
        }
    }
    return NULL;
}
 
void* do_inserts(void* arg) {
    st_en ste = *(st_en*) arg;
    int start = ste.start;
    const int end = ste.end;
    for (; start < end; start++) {
        char buf1[100], buf2[100];
        sprintf(buf1, "%d", start);
        sprintf(buf2, "for key = %d, value = %d", start, start + 1);

        MapPut(hm, buf1, buf2, strlen(buf2));
    }
    return NULL;
}
int max(int a, int b) {return a > b ? a : b;}

int main()
{
    hm = MapInit();
    const int MAP_SZ = 4e5;
    pthread_t* threads;
    const int N_WRITE_THREADS = 4;
    st_en* stes = (st_en*) malloc(sizeof(st_en) * max(N_WRITE_THREADS, N_READ_THREADS));
    {
        threads = (pthread_t*) malloc(sizeof(pthread_t) * N_WRITE_THREADS);
        fprintf(stderr, "writing %d kv pairs to hashmap from %d threads\n", MAP_SZ, N_WRITE_THREADS);
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);
        for (int i = 0; i < N_WRITE_THREADS; i++) {
            st_en* ste = stes + i;
            ste->start = (i * MAP_SZ) / N_WRITE_THREADS;
            ste->end = ((i + 1) * MAP_SZ) / N_WRITE_THREADS;
            pthread_create(threads + i, NULL, do_inserts, ste);
        }
        for (int i = 0; i < N_WRITE_THREADS; i++) {
            pthread_join(threads[i], NULL);
        }
        clock_gettime(CLOCK_MONOTONIC, &end);
        double cpu_time_used = (end.tv_sec - start.tv_sec);
        cpu_time_used += (end.tv_nsec - start.tv_nsec) / 1000000000.0;
     
        fprintf(stderr, "writes to hashmap completed in %lf s\n", cpu_time_used);
        free(threads);
    }

    threads = (pthread_t*) malloc(sizeof(pthread_t) * N_READ_THREADS);
    struct timespec start, end;
    fprintf(stderr, "reading %d keys from hashmap via %d threads\n", MAP_SZ * 2, N_READ_THREADS);
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < N_READ_THREADS; i++) {
        st_en* ste = stes + i;
        ste->start = (i * MAP_SZ) / N_READ_THREADS;
        ste->end = ((i + 1) * MAP_SZ) / N_READ_THREADS;
        pthread_create(threads + i, NULL, do_reads, ste);
    }
    for (int i = 0; i < N_READ_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    double cpu_time_used = (end.tv_sec - start.tv_sec);
    cpu_time_used += (end.tv_nsec - start.tv_nsec) / 1000000000.0;
 
    if (test_fail) {
        fprintf(stderr, "WARN: tests failed. 0 pts will be awarded for this\n");
        printf("reads: -1\n");
        return -1;
    } else {
        fprintf(stderr, "reads to hashmap completed in %lf s\n", cpu_time_used);
        printf("%lf\n", cpu_time_used);
    }
    free(threads);
    free(stes);
    return 0;
}
