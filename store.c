#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <db.h>
#include <sys/stat.h>
#include <errno.h>

#include "data.h"

static char* get_env_name(char *hashfile_name){
    /* split hashfile_name */
    char *token; 
    char *pch = strtok(hashfile_name, "/");
    while(pch != NULL){
        token = pch;
        pch = strtok(NULL, "/");
    }
    token = strtok(token, ".");
    return token;
}

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

int open_database(char* hashfile_name){

    int ret = init_ddb();

    char buf[100];
    strncpy(buf, hashfile_name, 99);
    char *env_name = get_env_name(buf);

    ret = open_ddb(env_name, 0);

    return ret;
}

int create_database(char *hashfile_name){

    int ret = init_ddb();

    char buf[100];
    strncpy(buf, hashfile_name, 99);
    char *env_name = get_env_name(buf);

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
        sizeof(r->csize) + sizeof(r->cratio) + r->rcount * sizeof(int);

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
    int i = 0;
    for(; i < r->rcount; i++){
        memcpy(value->data + off, &r->llist[i], sizeof(int));
        off += sizeof(int);
    }
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
    if(r->llist == NULL){
        r->lsize = r->rcount + 1;
        r->llist = malloc(sizeof(int)*r->lsize);
    }else if(r->lsize < r->rcount){
        r->lsize = r->rcount + 1;
        r->llist = realloc(r->llist, sizeof(int)*r->lsize);
    }
    int i = 0;
    for(; i < r->rcount; i++){
        memcpy(&r->llist[i], value->data + len, sizeof(int));
        len += sizeof(int);
    }
    assert(len == value->size);
}

/* 
 * Search hash in database.
 * chunk_rec returns a pointer if exists; otherwise, returns NULL
 * */
int search_chunk(struct chunk_rec *crec){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = crec->hash;
    key.size = crec->hashlen;

    value.data = NULL;
    value.size = 0;
    value.flags = DB_DBT_MALLOC;

    int ret = ddb.chunk_dbp->get(ddb.chunk_dbp, NULL, &key, &value, 0);
    if(ret == 0){
        /* success */
        unserial_chunk_rec(&value, crec);
        /*fprintf(stdout, "exist\n");*/
    }else if(ret == DB_NOTFOUND){
        assert(value.data == NULL);
        reset_chunk_rec(crec);
        /*fprintf(stdout, "Not exist\n");*/
    }else{
        fprintf(stderr, "Cannot get value: %s\n", db_strerror(ret));
    }

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
        fprintf(stderr, "Cannot put value: %s\n", db_strerror(ret));
    }
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

    int ret = ddb.container_dbp->put(ddb.region_dbp, NULL, &key, &value, 0);

    if(ret != 0){
        fprintf(stderr, "Cannot put value: %s\n", db_strerror(ret));
    }

    return ret;
}

int update_file(struct file_rec* r){
    DBT key, value;
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));

    key.data = &r->fid;
    key.size = sizeof(r->fid);

    value.data = r;
    value.size = sizeof(*r);

    int ret = ddb.file_dbp->put(ddb.file_dbp, NULL, &key, &value, 0);
    if(ret != 0){
        fprintf(stderr, "Cannot put value: %s\n", db_strerror(ret));
    }

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

int iterate_chunk(struct chunk_rec* r){
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

    return ret;
}


