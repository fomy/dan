#ifndef STORE_H_
#define STORE_H_

#include "data.h"

void create_database();
void open_database();
void close_database();

int search_chunk(struct chunk_rec *r);
int search_chunk_local(struct chunk_rec *r);
void update_chunk(struct chunk_rec *r);
int64_t get_chunk_number();

int search_file(struct file_rec* r);
void update_file(struct file_rec* r);
int64_t get_file_number();

void init_iterator(char *type);
void close_iterator();
int iterate_chunk(struct chunk_rec* r, int dedup_fid);
int iterate_file(struct file_rec* r);

#endif
