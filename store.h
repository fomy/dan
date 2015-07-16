#ifndef STORE_H_
#define STORE_H_

#include "data.h"

void create_database();
void open_database();
void close_database();

int search_chunk(struct chunk_rec *crec);
void update_chunk(struct chunk_rec *crec);

int search_container(struct container_rec *r);
void update_container(struct container_rec* r);

int search_region(struct region_rec* r);
void update_region(struct region_rec* r);

int search_file(struct file_rec* r);
void update_file(struct file_rec* r);

void init_iterator(char *type);
void close_iterator();
int iterate_chunk(struct chunk_rec* r, int dedup_fid);
int iterate_container(struct container_rec* r);
int iterate_region(struct region_rec* r);
int iterate_file(struct file_rec* r);

#endif
