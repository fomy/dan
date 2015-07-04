#ifndef STORE_H_
#define STORE_H_

#include "data.h"

int create_database(char *hashfile_name);
int open_database(char *hashfile_name);
void close_database();

int search_chunk(struct chunk_rec *crec);
int update_chunk(struct chunk_rec *crec);

int search_container(struct container_rec *r);
int update_container(struct container_rec* r);

int search_region(struct region_rec* r);
int update_region(struct region_rec* r);

int update_file(struct file_rec* r);

int init_iterator(char *type);
int iterate_chunk(struct chunk_rec* r);

#endif
