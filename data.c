#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "data.h"

/* Use memset(r, 0, sizeof(*r)) to init */

void reset_chunk_rec(struct chunk_rec *r)
{
    r->rcount = 0;
    r->cid = 0;
    r->rid = 0;
    r->csize = 0;
    r->cratio = 0;
}

void free_chunk_rec(struct chunk_rec *r)
{
    if(r->list) {
        free(r->list);
		r->list = NULL;
	}
    free(r);
}

void copy_chunk_rec(struct chunk_rec *r, struct chunk_rec *copy)
{
	copy->hashlen = r->hashlen;
	memcpy(copy->hash, r->hash, r->hashlen);
	copy->rcount = r->rcount;
	copy->cid = r->cid;
	copy->rid = r->rid;
	copy->csize = r->csize;
	copy->cratio = r->cratio;

	if(copy->list == NULL) {
		copy->list = malloc(sizeof(int) * copy->rcount * 2);
		copy->listsize = copy->rcount * 2;
	} else if (copy->listsize > copy->rcount * 4 
			|| copy->listsize < copy->rcount * 2) {
		copy->list = realloc(copy->list, sizeof(int) * copy->rcount * 2);
		copy->listsize = copy->rcount * 2;
	}
	memset(copy->list, 0, copy->listsize);
	memcpy(copy->list, r->list, r->rcount * 2);
}

void reset_container_rec(struct container_rec *r)
{
    memset(r, 0, sizeof(*r));
    r->slots = REGIONS_PER_CONTAINER;
}

int container_full(struct container_rec* r)
{
    return r->slots == 0? 1 : 0;
}

void reset_region_rec(struct region_rec* r)
{
    memset(r, 0, sizeof(*r));
}

static inline int region_overflow(struct region_rec* r, int csize)
{
    return r->psize + csize > COMPRESSION_REGION_SIZE? 1:0;
}

/* return 1: success; return 0: overflow */
int add_chunk_to_region(struct chunk_rec* c, struct region_rec* r)
{
    if(region_overflow(r, c->csize))
        return 0;

    r->psize += c->csize;
    r->lsize += c->csize;
    return 1;
}

int add_region_to_container(struct region_rec* r, struct container_rec* c)
{
    if(container_full(c))
        return 0;

    c->lsize += r->lsize;
    c->psize += r->psize;
    c->slots--;
    return 1;
}

void parse_file_suffix(char *path, char *suffix, int suffixlen)
{
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

void free_file_rec(struct file_rec *r)
{
    if(r->fname)
        free(r->fname);
    free(r);
}

void copy_file_rec(struct file_rec *r, struct file_rec *c)
{
	c->fid = r->fid;
	c->cnum = r->cnum;
	c->fsize = r->fsize;
	memcpy(c->hash, r->hash, 20);
	memcpy(c->minhash, r->minhash, 20);
	memcpy(c->maxhash, r->maxhash, 20);

	if (c->fname == NULL) {
		c->fname = malloc(strlen(r->fname) + 1);
	} else {
		c->fname = realloc(c->fname, strlen(r->fname) + 1);
	}
	strcpy(c->fname, r->fname);
}
