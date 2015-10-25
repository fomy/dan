#ifndef STORE_H_
#define STORE_H_

#include "data.h"

#define STORE_EXISTED 0
#define STORE_NOTFOUND 1

#define ITER_STOP 0
#define ITER_CONTINUE 1

void open_database(char* dbhome);
void close_database();

int search_chunk(struct chunk_rec *r);
int search_chunk_local(struct chunk_rec *r);
void update_chunk(struct chunk_rec *r);
int get_chunk_number();

int search_file(struct file_rec* r);
void insert_file(struct file_rec* r);
int get_file_number();

void init_iterator(char *type);
void close_iterator(char *type);
int iterate_chunk(struct chunk_rec* r);
int iterate_file(struct file_rec* r);

void print_store_stat();

#endif
