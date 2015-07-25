#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "data.h"

/* Use memset(r, 0, sizeof(*r)) to init */

void reset_chunk_rec(struct chunk_rec *r){
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
    r->fcount = 0;
}

void free_chunk_rec(struct chunk_rec *r){
    if(r->list)
        free(r->list);
    free(r);
}

void reset_container_rec(struct container_rec *r){
    memset(r, 0, sizeof(*r));
    r->slots = REGIONS_PER_CONTAINER;
}

int container_full(struct container_rec* r){
    return r->slots == 0? 1 : 0;
}

void reset_region_rec(struct region_rec* r){
    memset(r, 0, sizeof(*r));
}

static inline int region_overflow(struct region_rec* r, int csize){
    return r->psize + csize > COMPRESSION_REGION_SIZE? 1:0;
}

/* return 1: success; return 0: overflow */
int add_chunk_to_region(struct chunk_rec* c, struct region_rec* r){
    if(region_overflow(r, c->csize))
        return 0;

    r->psize += c->csize;
    r->lsize += c->csize;
    return 1;
}

int add_region_to_container(struct region_rec* r, struct container_rec* c){
    if(container_full(c))
        return 0;

    c->lsize += r->lsize;
    c->psize += r->psize;
    c->slots--;
    return 1;
}

void parse_file_suffix(char *path, char *suffix, int suffixlen){
    int i = strlen(path) - 1;
    while(i>=0 && path[i]!= '.' && path[i]!='\\' && path[i]!='/')
        i--;
    if(i<0 || path[i] == '\\' || path[i] == '/')
        memset(suffix, 0, suffixlen);
    else{
        assert(path[i] == '.');
        strncpy(suffix, &path[i+1], suffixlen);
        suffix[suffixlen-1] = 0;
    }
}

void free_file_rec(struct file_rec *r){
    if(r->fname)
        free(r->fname);
    free(r);
}
