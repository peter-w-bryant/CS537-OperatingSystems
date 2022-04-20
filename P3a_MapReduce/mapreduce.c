#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include "mapreduce.h"
#include "hashmap.h"

struct map_struct{
    void (*p)(char*);
    char** filename;
    int iter_count;
};

struct reduce_struct{
    void (*p)(char*,Getter,int);
    char** key;
    Getter get_func;
    int partition_number;
};

struct intermediate_kp{
    char* key;
    int partition_id;
    int key_id;
};

struct kv {
    char* key;
    char* value;
    int partition_id;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

struct kv_list kvl;
size_t num_keys;
pthread_mutex_t map_mutex, reduce_mutex;
struct intermediate_kp* kp = NULL;

void init_kv_list(size_t size) {
    kvl.elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl.num_elements = 0;
    kvl.size = size;
    num_keys = 0;
    pthread_mutex_init(&map_mutex,NULL);
    pthread_mutex_init(&reduce_mutex,NULL);
}

void add_to_list(struct kv* elt) {
    if (kvl.num_elements == kvl.size) {
        kvl.size *= 2;
        kvl.elements = realloc(kvl.elements, kvl.size * sizeof(struct kv*));
    }
    kvl.elements[kvl.num_elements++] = elt;

    //int i,flag;
    //flag = 1;
    //for(i=0; i<kvl.num_elements-1; i++){
    // if(!strcmp(elt->key,kvl.elements[i]->key))
    //  flag = 0;
    //}
    //num_keys += flag;
}

char* get_func(char* key, int partition_number) {
    //if (kvl_counter == kvl.num_elements) {
    //    return NULL;
    //}
    //struct kv *curr_elt = kvl.elements[kvl_counter];
    //if (!strcmp(curr_elt->key, key)) {
    //    kvl_counter++;
    //    return curr_elt->value;
    //}
    //return NULL;

    int i=0;
    char* temp = NULL;
    //pthread_mutex_lock(&reduce_mutex); //TODO: Think twice whether this is needed?
    //for(;i<kvl.num_elements; i++){
    // if(partition_number == kvl.elements[i]->partition_id && !strcmp(kvl.elements[i]->key,key)){
    //  kvl.elements[i]->partition_id = -1;
    //  temp = kvl.elements[i]->value;
    //  //pthread_mutex_unlock(&reduce_mutex);
    //  return temp;
    // }
    //}

    for(;i<num_keys && kp[i].key_id<kvl.num_elements; i++)
     if(!strcmp(kvl.elements[kp[i].key_id]->key,key) && !strcmp(kp[i].key,key)){
      temp = kvl.elements[kp[i].key_id]->value;
      kp[i].key_id++;
      return temp;
     }
    //pthread_mutex_unlock(&reduce_mutex);
    return NULL;
}

int cmp(const void* a, const void* b) {
    char* str1 = (*(struct kv **)a)->key;
    char* str2 = (*(struct kv **)b)->key;
    return strcmp(str1, str2);
}

void MR_Emit(char* key, char* value)
{
    struct kv *elt = (struct kv*) malloc(sizeof(struct kv));
    if (elt == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        exit(1);
    }
    elt->key = strdup(key);
    elt->value = strdup(value);
    pthread_mutex_lock(&map_mutex);
    add_to_list(elt);
    pthread_mutex_unlock(&map_mutex);

    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void* map_wrapper(void* pointer){
   struct map_struct* ms = (struct map_struct*)pointer;
   int i = 0;
   while(i<ms->iter_count){
    (ms->p)(ms->filename[i]);
    i++;
   }
   return 0;
}

void* reduce_wrapper(void* pointer){
   struct reduce_struct* rs = (struct reduce_struct*)pointer;
   int i = 0;
   while(rs->key[i]){
    (rs->p)(rs->key[i],rs->get_func,rs->partition_number);
    i++;
   }
   return 0;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Partitioner partition)
{

    const int num_map_thread = num_mappers;
    const int num_red_thread = num_reducers;

    pthread_t map_thread_id[num_map_thread];
    pthread_t red_thread_id[num_red_thread];

    init_kv_list(10); //TODO

    int actual_mapper_count, mapper_iter_count;

    if(num_mappers >= (argc-1)){
     actual_mapper_count = argc-1;
     mapper_iter_count = 0;
    }
    else{
     mapper_iter_count = (argc-1) / num_mappers;
     actual_mapper_count = (argc-1) % num_mappers;
     //mapper_iter_count += (actual_mapper_count>0)?1:0;
    }
    struct map_struct* ms = NULL;
    ms = (struct map_struct*)malloc(num_mappers * sizeof(*ms));

    int i,j,k,l;
    j=0; l = 1;
    for (i = 0; i < (mapper_iter_count>0?num_mappers:actual_mapper_count) ; i++) { //Round Robin as of now
        //(*map)(argv[i]);
        ms[i].p = map;
        ms[i].iter_count = mapper_iter_count + ((i<actual_mapper_count)?1:0) ;
        k=0;
	ms[i].filename = (char**)malloc(ms[i].iter_count*sizeof(char*));
        while(k<ms[i].iter_count)
         ms[i].filename[k++] = argv[l++];
        pthread_create(&map_thread_id[i], NULL, map_wrapper, &ms[i]);
    }

    for(i=0; i < (mapper_iter_count>0?num_mappers:actual_mapper_count); i++){
        pthread_join(map_thread_id[i],NULL);
	free(ms[i].filename);
    }
    free(ms);

    qsort(kvl.elements, kvl.num_elements, sizeof(struct kv*), cmp);

    num_keys = 0;
    for(i=1; i<kvl.num_elements; i++){
     if(strcmp(kvl.elements[i-1]->key,kvl.elements[i]->key))
      num_keys++;
    }
    if(kvl.num_elements>0)
     num_keys++;
    
    kp = (struct intermediate_kp*)malloc(num_keys*sizeof(*kp));

    if(num_keys>0){
     j=0; i=0;
     kvl.elements[i]->partition_id = (*partition)(kvl.elements[i]->key,num_reducers);
     kp[j].partition_id = kvl.elements[i]->partition_id;
     kp[j].key = kvl.elements[i]->key;
     kp[j].key_id = 0;
     j++; 
     for(i=1; i< kvl.num_elements; i++){
      kvl.elements[i]->partition_id = (*partition)(kvl.elements[i]->key,num_reducers);
      if(strcmp(kvl.elements[i-1]->key,kvl.elements[i]->key)){
       kp[j].partition_id = kvl.elements[i]->partition_id;
       kp[j].key = kvl.elements[i]->key;
       kp[j].key_id = i;
       j++;
      }
     }
    }

    //kvl_counter = 0;
    // while (kvl_counter < kvl.num_elements) {
    //    (*reduce)((kvl.elements[kvl_counter])->key, get_func, 0);
    //}

    struct reduce_struct* rs = NULL;
    int* flag = NULL;
    rs = (struct reduce_struct*)malloc(num_reducers * sizeof(*rs));
    flag = (int *)malloc(num_reducers*sizeof(int));
 
    for(i=0; i<num_reducers; i++){
     rs[i].p = reduce;
     rs[i].get_func = get_func;
     rs[i].partition_number = i;
     k=0;
     for(j=0; j<num_keys;j++)
      if(kp[j].partition_id==i)
       k++;
     rs[i].key = (char**)malloc((k+1)*sizeof(char*));
     rs[i].key[k] = NULL;
     k=0;
     for(j=0; j<num_keys; j++){
      if(kp[j].partition_id==i){
       rs[i].key[k] = kp[j].key;
       k++;
      }
     }
     flag[i] = 0;
     if(k>0){
      pthread_create(&red_thread_id[i],NULL, reduce_wrapper, &rs[i]);
      flag[i] = 1;
     }
    }
 
    for(i=0; i<num_reducers; i++){
     if(flag[i])
      pthread_join(red_thread_id[i],NULL);
    }
    for(i=0; i<num_reducers; i++)
     free(rs[i].key);
    free(rs);
    free(flag);

    //TODO: Free the entire kvl data structure
    free(kp);
    free(kvl.elements);
}
