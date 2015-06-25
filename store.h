#ifndef STORE_H_
#define STORE_H_

#include "data.h"

int open_database(char *hashfile_name);
void close_database();
int search_chunk(struct chunk_rec *crec);
int update_chunk(struct chunk_rec *crec);

#endif
