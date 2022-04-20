#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);

typedef void (*Mapper)(char *file_name);
// `get_state` and `get_next` will only be called once inside the reducer in eager mode!
// `get_state` is NULL in simple mode and `get_next` can be called until you get NULL.
typedef void (*Reducer)(char *key, Getter get_next, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what *you must implement*
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition);

#endif // __mapreduce_h__
