#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include "mapreduce.h"

#ifndef NUM_MAPPERS
    #define NUM_MAPPERS 1
#endif

#ifndef NUM_REDUCERS
    #define NUM_REDUCERS 1
#endif

#ifndef FILE_OUTPUT_SUFFIX
    #define FILE_OUTPUT_SUFFIX ""
#endif

int output_fd[NUM_REDUCERS];

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, "., \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter getter,
            int partition_number) {

    int count = 0;
    char *value;
    while ((value = getter(key, partition_number)) != NULL) {
        count += atoi(value);
    }

    value = (char*)malloc(12 * sizeof(char));
    sprintf(value, "%d", count); // Convert integer to string

    dprintf(output_fd[partition_number],"%s %d\n", key, count);
    free(value);
}

int main(int argc, char *argv[]) {
    char buf[64];
    for(int i=0;i<NUM_REDUCERS;i++){
    	sprintf(buf, "wordcount_%s(%d).out", FILE_OUTPUT_SUFFIX,i);
    	if( (output_fd[i] = open(buf, O_CREAT|O_TRUNC|O_WRONLY,0664)) < 0 ){
    	    perror("file open error");
    	    exit(1);
    	}
    }

    struct timespec start, end;
    double cpu_time_used;
    clock_gettime(CLOCK_MONOTONIC, &start);

    MR_Run(argc, argv, Map, NUM_MAPPERS, Reduce, NUM_REDUCERS, MR_DefaultHashPartition);

    clock_gettime(CLOCK_MONOTONIC, &end);
    cpu_time_used = (end.tv_sec - start.tv_sec);
    cpu_time_used += (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("** %.3f secs elapsed **\n", cpu_time_used);

    for(int i=0;i<NUM_REDUCERS;i++){
        if( close(output_fd[i]) < 0 ){
            perror("file open error");
            exit(1);
        }
    }
}
