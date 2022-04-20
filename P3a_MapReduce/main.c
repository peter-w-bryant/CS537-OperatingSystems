#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "mapreduce.h"
#include "hashmap.h"

HashMap* hashmap;

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
	    if (!strcmp(token, ""))
		break;
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    // HashMap take a (void *) as value
    int *count = (int*)malloc(sizeof(int));
    *count = 0;
    char *value;
    
    while ((value = get_next(key, partition_number)) != NULL)
        (*count)++;

    MapPut(hashmap, key, count, sizeof(int));
}

/* This program accepts a list of files and stores their words and
 * number of occurrences in a hashmap. After populating the hashmap,
 * it is search for a word (searchterm) and the number of occurrences
 * is printed. */

int main(int argc, char *argv[]) {
    if (argc < 3) {
	printf("Invalid usage: ./hashmap <filename> ... <searchterm>\n");
	return 1;
    }
    
    hashmap = MapInit();
    // save the searchterm
    char* searchterm = argv[argc - 1];
    argc -= 1;

    // run mapreduce
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    // get the number of occurrences and print
    char *result;
    if ((result = MapGet(hashmap, searchterm)) != NULL) {
	printf("Found %s %d times\n", searchterm, *(int*)result);
    } else {
	printf("Word not found!\n");
    }
    
    return 0;
}
