#include <stdlib.h>
#include "data.h"

/* memset(r, 0, sizeof(*r)) */
void init_chunk_rec(struct chunk_rec *r){

    r->hashlen = 0;
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
    r->lsize = 0;
    r->llist = NULL;

}

void reset_chunk_rec(struct chunk_rec *r){
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
}

void init_container_rec(struct container_rec *r){

    r->cid = -1;
    r->lsize = 0;
    r->psize = 0;
}

int container_full(struct container_rec* r, int csize){
    if(r->psize + csize > CONTAINER_SIZE){
        return 1;
    }
    return 0;
}

void init_region_rec(struct region_rec *r){

    r->rid = -1;
    r->lsize = 0;
    r->psize = 0;
}

int region_full(struct region_rec* r, int csize){
    if(r->psize + csize > COMPRESSION_REGION_SIZE){
        return 1;
    }
    return 0;
}
