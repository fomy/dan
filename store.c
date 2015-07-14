#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <db.h>
#include <sys/stat.h>
#include <errno.h>

#include "data.h"

static DB* dbp;

struct {
    DB_ENV *env;
    DB* chunk_dbp;
    DB* container_dbp;
    DB* region_dbp;
    DB* file_dbp;
    DBC* cursorp;
} ddb;

static int init_ddb(){
    ddb.cursorp = NULL;
    /* create and open env */
    int ret = db_env_create(&ddb.env, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot create ENV: %s\n", db_strerror(ret));
        return ret;
    }

    /* 2GB cache */
    ret = ddb.env->set_cachesize(ddb.env, 2, 0, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot set cache for ENV: %s\n", db_strerror(ret));
        return ret;
    }

    /* create db */
    ret = db_create(&ddb.chunk_dbp, ddb.env, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot create chunk DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = db_create(&ddb.container_dbp, ddb.env, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot create container DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = db_create(&ddb.region_dbp, ddb.env, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot create region DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = db_create(&ddb.file_dbp, ddb.env, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot create file DB: %s\n", db_strerror(ret));
        return ret;
    }
    return 0;
}

static int open_ddb(char* env_name, int flags){
    int ret = ddb.env->open(ddb.env, env_name, flags|DB_INIT_MPOOL, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot open ENV: %s\n", db_strerror(ret));
        return ret;
    }

    /* open db */ 
    ret = ddb.chunk_dbp->open(ddb.chunk_dbp, NULL, "chunk.db", NULL, DB_HASH, flags, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot open chunk DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = ddb.container_dbp->open(ddb.container_dbp, NULL, "container.db", NULL, DB_HASH, flags, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot open container DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = ddb.region_dbp->open(ddb.region_dbp, NULL, "region.db", NULL, DB_HASH, flags, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot open region DB: %s\n", db_strerror(ret));
        return ret;
    }

    ret = ddb.file_dbp->open(ddb.file_dbp, NULL, "file.db", NULL, DB_HASH, flags, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot open file DB: %s\n", db_strerror(ret));
        return ret;
    }
    return 0;
}

int open_database(char* env_name){

    int ret = init_ddb();

    ret = open_ddb(env_name, 0);

    return ret;
}

int create_database(char *env_name){

    int ret = init_ddb();

    ret = mkdir(env_name, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if(ret != 0){
        fprintf(stderr, "Cannot create ENV directory: %s\n", strerror(errno));
        /*return ret;*/
    }

    ret = open_ddb(env_name, DB_CREATE);
    return ret;
}

void close_database(){
    ddb.chunk_dbp->close(ddb.chunk_dbp, 0);
    ddb.container_dbp->close(ddb.container_dbp, 0);
    ddb.region_dbp->close(ddb.region_dbp, 0);
    ddb.file_dbp->close(ddb.file_dbp, 0);

    ddb.env->close(ddb.env, 0);
}

static void serial_chunk_rec(struct chunk_rec* r, DBT* value){
    
    value->size = sizeof(r->rcount) + sizeof(r->cid) + sizeof(r->rid) +
        sizeof(r->csize) + sizeof(r->cratio) + sizeof(r->fcount) +  2 * r->rcount * sizeof(int);

    value->data = malloc(value->size);

    int off = 0;
    memcpy(value->data + off, &r->rcount, sizeof(r->rcount));
    off += sizeof(r->rcount);
    memcpy(value->data + off, &r->cid, sizeof(r->cid));
    off += sizeof(r->cid);
    memcpy(value->data + off, &r->rid, sizeof(r->rid));
    off += sizeof(r->rid);
    memcpy(value->data + off, &r->csize, sizeof(r->csize));
    off += sizeof(r->csize);
    memcpy(value->data + off, &r->cratio, sizeof(r->cratio));
    off += sizeof(r->cratio);
    memcpy(value->data + off, &r->fcount, sizeof(r->fcount));
    off += sizeof(r->fcount);

    memcpy(value->data + off, r->list, sizeof(int) * r->rcount);
    off += sizeof(int) * r->rcount;
    memcpy(value->data + off, &r->list[r->lsize/2], sizeof(int) * r->rcount);
    off += sizeof(int) * r->rcount;

    assert(off == value->size);
}

static void unserial_chunk_rec(DBT *value, struct chunk_rec *r){

    int len = 0;
    memcpy(&r->rcount, value->data, sizeof(r->rcount));
    len += sizeof(r->rcount);
    memcpy(&r->cid, value->data + len, sizeof(r->cid));
    len += sizeof(r->cid);
    memcpy(&r->rid, value->data + len, sizeof(r->rid));
    len += sizeof(r->rid);
    memcpy(&r->csize, value->data + len, sizeof(r->csize));
    len += sizeof(r->csize);
    memcpy(&r->cratio, value->data + len, sizeof(r->cratio));
    len += sizeof(r->cratio);
    memcpy(&r->fcount, value->data + len, sizeof(r->fcount));
    len += sizeof(r->fcount);
    if(r->list == NULL){
        r->lsize = r->rcount * 2 + 2;
        r->list = malloc(sizeof(int) * r->lsize);
    }else if(r->lsize < r->rcount * 2 + 2 ){
        r->lsize = r->rcount * 2 + 2;
        r->list = realloc(r->list, sizeof(int) * r->lsize);
    }

    memcpy(r->list, value->data + len, sizeof(int) * r->rcount);
    len += sizeof(int) * r->rcount;
    memcpy(&r->list[r->lsize/2], value->data + len, sizeof(int) * r->rcount);
    len += sizeof(int) * r->rcount;

    assert(len == value->size);
}

/* 
 * Search hash in database.
 * chunk_rec returns a pointer if exists; otherwise, returns NULL
 * return 1 indicates existence; 0 indicates not exist.
 * */
int search_chunk(struct chunk_rec *crec){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = crec->hash;
    key.size = crec->hashlen;
    /* Berkeley DB will not write into the DBT. See API reference.
     * I am not sure whether DB will write to key by default. */
    key.flags = DB_DBT_READONLY;

    value.data = NULL;
    value.size = 0;
    /* Berkeley DB will allocate memory for the returned item.
     * We should free it by ourselves */
    value.flags = DB_DBT_MALLOC;

    int ret = ddb.chunk_dbp->get(ddb.chunk_dbp, NULL, &key, &value, 0);
    if(ret == 0){
        /* success */
        unserial_chunk_rec(&value, crec);
        /*fprintf(stdout, "exist\n");*/
        ret = 1;
    }else if(ret == DB_NOTFOUND){
        assert(value.data == NULL);
        reset_chunk_rec(crec);
        /*fprintf(stdout, "Not exist\n");*/
        ret = 0;
    }else{
        fprintf(stderr, "Cannot get value: %s\n", db_strerror(ret));
        ret = -1;
    }

    free(value.data);

    return ret;
}

int update_chunk(struct chunk_rec *crec){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = crec->hash;
    key.size = crec->hashlen;

    serial_chunk_rec(crec, &value);
    int ret = ddb.chunk_dbp->put(ddb.chunk_dbp, NULL, &key, &value, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot put value: %s\n", db_strerror(ret));
    }
    free(value.data);
    return ret;
}

int search_container(struct container_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->cid;
    key.size = sizeof(r->cid);
    key.flags = DB_DBT_READONLY;

    value.data = NULL;
    value.size = 0;
    value.flags = DB_DBT_MALLOC;

    int ret = ddb.container_dbp->get(ddb.container_dbp, NULL, &key, &value, 0);
    if(ret == 0){
        /* success */

        struct container_rec *v = value.data;
        r->lsize = v->lsize;
        r->psize = v->psize;
        assert(r->cid == v->cid);

        /*fprintf(stdout, "exist\n");*/
        ret = 1;
    }else if(ret == DB_NOTFOUND){
        assert(value.data == NULL);
        /*fprintf(stdout, "Not exist\n");*/
        ret = 0;
    }else{
        fprintf(stderr, "Cannot get container: %s\n", db_strerror(ret));
        ret = -1;
    }

    free(value.data);

    return ret;
}

int update_container(struct container_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->cid;
    key.size = sizeof(r->cid);

    value.data = r;
    value.size = sizeof(*r);

    int ret = ddb.container_dbp->put(ddb.container_dbp, NULL, &key, &value, 0);

    if(ret != 0){
        fprintf(stderr, "Cannot put container: %s\n", db_strerror(ret));
    }
    return ret;
}

int search_region(struct region_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->rid;
    key.size = sizeof(r->rid);
    key.flags = DB_DBT_READONLY;

    value.data = NULL;
    value.size = 0;
    value.flags = DB_DBT_MALLOC;

    int ret = ddb.region_dbp->get(ddb.region_dbp, NULL, &key, &value, 0);
    if(ret == 0){
        /* success */
        struct region_rec *v = value.data;
        r->lsize = v->lsize;
        r->psize = v->psize;
        assert(r->rid == v->rid);

        /*fprintf(stdout, "exist\n");*/
        ret = 1;
    }else if(ret == DB_NOTFOUND){
        assert(value.data == NULL);
        /*fprintf(stdout, "Not exist\n");*/
        ret = 0;
    }else{
        fprintf(stderr, "Cannot get region: %s\n", db_strerror(ret));
        ret = -1;
    }

    free(value.data);

    return ret;
}

int update_region(struct region_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->rid;
    key.size = sizeof(r->rid);

    value.data = r;
    value.size = sizeof(*r);

    int ret = ddb.region_dbp->put(ddb.region_dbp, NULL, &key, &value, 0);

    if(ret != 0){
        fprintf(stderr, "Cannot put region: %s\n", db_strerror(ret));
    }

    return ret;
}

static void serial_file_rec(struct file_rec* r, DBT* value){
    
    value->size = sizeof(r->fid) + sizeof(r->cnum) + sizeof(r->fsize) +
        sizeof(r->hash) + sizeof(r->minhash) + sizeof(r->maxhash) + strlen(r->fname);

    assert(value->data == NULL);
    value->data = malloc(value->size);

    int off = 0;
    memcpy(value->data + off, &r->fid, sizeof(r->fid));
    off += sizeof(r->fid);
    memcpy(value->data + off, &r->cnum, sizeof(r->cnum));
    off += sizeof(r->cnum);
    memcpy(value->data + off, &r->fsize, sizeof(r->fsize));
    off += sizeof(r->fsize);
    memcpy(value->data + off, r->hash, sizeof(r->hash));
    off += sizeof(r->hash);
    memcpy(value->data + off, r->minhash, sizeof(r->minhash));
    off += sizeof(r->minhash);
    memcpy(value->data + off, r->maxhash, sizeof(r->maxhash));
    off += sizeof(r->maxhash);
    memcpy(value->data + off, r->fname, strlen(r->fname));
}

static void unserial_file_rec(DBT *value, struct file_rec *r){

    int len = 0;
    memcpy(&r->fid, value->data, sizeof(r->fid));
    len += sizeof(r->fid);
    memcpy(&r->cnum, value->data + len, sizeof(r->cnum));
    len += sizeof(r->cnum);
    memcpy(&r->fsize, value->data + len, sizeof(r->fsize));
    len += sizeof(r->fsize);
    memcpy(r->hash, value->data + len, sizeof(r->hash));
    len += sizeof(r->hash);
    memcpy(r->minhash, value->data + len, sizeof(r->minhash));
    len += sizeof(r->minhash);
    memcpy(r->maxhash, value->data + len, sizeof(r->maxhash));
    len += sizeof(r->maxhash);

    if(r->fname == NULL)
        r->fname = malloc(value->size - len + 1);
    else
        r->fname = realloc(r->fname, value->size - len + 1);

    memcpy(r->fname, value->data + len, value->size - len);
    r->fname[value->size - len] = 0;

}
int search_file(struct file_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->fid;
    key.size = sizeof(r->fid);
    key.flags = DB_DBT_READONLY;

    value.data = NULL;
    value.size = 0;
    value.flags = DB_DBT_MALLOC;

    int ret = ddb.file_dbp->get(ddb.file_dbp, NULL, &key, &value, 0);
    if(ret == 0){
        /* success */
        unserial_file_rec(&value, r);
        ret = 1;
    }else if(ret == DB_NOTFOUND){
        assert(value.data == NULL);
        /*fprintf(stdout, "Not exist\n");*/
        ret = 0;
    }else{
        fprintf(stderr, "Cannot get file: %s\n", db_strerror(ret));
        ret = -1;
    }

    free(value.data);

    return ret;
}

int update_file(struct file_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->fid;
    key.size = sizeof(r->fid);

    serial_file_rec(r, &value);

    int ret = ddb.file_dbp->put(ddb.file_dbp, NULL, &key, &value, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot put file: %s\n", db_strerror(ret));
    }

    free(value.data);
    return ret;
}

int init_iterator(char *type){
    int ret;
    if(strcmp(type, "CHUNK") == 0){
        ret = ddb.chunk_dbp->cursor(ddb.chunk_dbp, NULL, &ddb.cursorp, 0);
    }else if(strcmp(type, "CONTAINER") == 0){
        ret = ddb.container_dbp->cursor(ddb.container_dbp, NULL, &ddb.cursorp, 0);
    }else if(strcmp(type, "REGION") == 0){
        ret = ddb.region_dbp->cursor(ddb.region_dbp, NULL, &ddb.cursorp, 0);
    }else if(strcmp(type, "FILE") == 0){
        ret = ddb.file_dbp->cursor(ddb.file_dbp, NULL, &ddb.cursorp, 0);
    }else{
        ret = -1;
        fprintf(stderr, "invalid iterator type!\n");
    }
    return ret;
}

void close_iterator(){
    ddb.cursorp->close(ddb.cursorp);
}

/* dedup_fid = 1: remove duplicate file id in file list
 * dedup_fid = 0: do not remove */
int iterate_chunk(struct chunk_rec* r, int dedup_fid){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    int ret = ddb.cursorp->get(ddb.cursorp, &key, &value, DB_NEXT);
    if(ret != 0){
        fprintf(stderr, "no more chunk\n");
        return ret;
    }

    memcpy(r->hash, key.data, key.size);
    r->hashlen = key.size;

    unserial_chunk_rec(&value, r);

    int m = 0;
    for(; m<r->rcount; m++){
        printf("%d,", r->list[r->lsize/2+m]);
    }
    puts("");

    if(dedup_fid && r->rcount > r->fcount){
        int* list = &r->list[r->lsize/2];
        int step = 0, i;
        for(i=1; i<r->rcount; i++){
            printf("%d == %d\n", list[i], list[i-1]);
            if(list[i] == list[i-1]){
                step++;
                continue;
            }
            list[i-step] = list[i];
        }

        if(step != (r->rcount - r->fcount)){
            printf("%d, %d, %d\n", step, r->rcount, r->fcount);
            exit(-2);
        }
    }

    return ret;
}

int iterate_container(struct container_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    int ret = ddb.cursorp->get(ddb.cursorp, &key, &value, DB_NEXT);
    if(ret != 0){
        fprintf(stderr, "no more container\n");
        return ret;
    }

    struct container_rec* v = value.data;
    r->cid = v->cid;
    r->psize = v->psize;
    r->lsize = v->lsize;

    return ret;
}

int iterate_region(struct region_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    int ret = ddb.cursorp->get(ddb.cursorp, &key, &value, DB_NEXT);
    if(ret != 0){
        fprintf(stderr, "no more region\n");
        return ret;
    }

    struct region_rec *v = value.data;
    r->rid = v->rid;
    r->psize = v->psize;
    r->lsize = v->lsize;

    return ret;
}

int iterate_file(struct file_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    int ret = ddb.cursorp->get(ddb.cursorp, &key, &value, DB_NEXT);
    if(ret != 0){
        fprintf(stderr, "no more file\n");
        return ret;
    }

    unserial_file_rec(&value, r);

    return ret;
}
