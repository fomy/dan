#include <stdlib.h>
#include "data.h"

struct chunk_rec* create_chunk_rec(){
    struct chunk_rec* r = malloc(sizeof(struct chunk_rec));

    r->hashlen = 0;
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
    r->lnum = 0;
    r->lsize = 0;
    r->llist = NULL;

    return r;
}

void free_chunk_rec(struct chunk_rec *r){
    if(r->llist != NULL){
        free(r->llist);
        r->llist = NULL;
        r->lsize = 0;
    }
    free(r);
}

void reset_chunk_rec(struct chunk_rec *r){
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
    r->lnum = 0;
}

struct container_rec* create_container_rec(){
    struct container_rec* r = malloc(sizeof(struct container_rec));

    r->cid = -1;
    r->lsize = 0;
    r->psize = 0;

    return r;
}

void free_container_rec(struct container_rec* r){
    free(r);
}

struct region_rec* create_region_rec(){
    struct region_rec* r = malloc(sizeof(struct region_rec));

    r->rid = -1;
    r->lsize = 0;
    r->psize = 0;
}

void free_region_rec(struct region_rec* r){
    free(r);
}
