#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <errno.h>
#include <hiredis/hiredis.h>

#include "data.h"

typedef struct {
    void* data;
    size_t size;
} KVOBJ;

#define CHUNK_DB 1
#define REGION_DB 2
#define CONTAINER_DB 3
#define FILE_DB 4

static redisContext *redis = NULL;
static int current_db = CHUNK_DB;
static int CREATE = 0;

void open_database(){
    redis = redisConnect("127.0.0.1", 6379);
    if(redis != NULL && redis->err){
        fprintf(stderr, "Error: %s\n", redis->errstr);
        exit(-1);
    }
}

void create_database(){
    CREATE = 1;
    open_database();
}

void close_database(){
    if(CREATE == 1){
        redisReply *reply = redisCommand(redis, "SAVE");
        assert(reply->type == REDIS_REPLY_STATUS);
    }
    redisFree(redis);
}

static inline void select_db(int db){
    if(current_db != db){
        redisReply *reply = redisCommand(redis, "SELECT %d", db);
        assert(reply->type == REDIS_REPLY_STATUS);
        current_db = db;
    }
}

static void serial_chunk_rec(struct chunk_rec* r, KVOBJ* value){
    
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

static void unserial_chunk_rec(KVOBJ* value, struct chunk_rec *r){

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

    KVOBJ key, value;

    key.data = crec->hash;
    key.size = crec->hashlen;

    value.data = NULL;
    value.size = 0;

    select_db(CHUNK_DB);

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to GET chunk\n");
        exit(-1);
    }

    int ret = 0;
    if(reply->type == REDIS_REPLY_NIL){
        reset_chunk_rec(crec);
        ret = 0;
    }else{
        assert(reply->type == REDIS_REPLY_STRING);
        value.data = reply->str;
        value.size = reply->len;
        unserial_chunk_rec(&value, crec);
        ret = 1;
    }

    freeReplyObject(reply);

    return ret;
}

void update_chunk(struct chunk_rec *crec){
    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = crec->hash;
    key.size = crec->hashlen;

    serial_chunk_rec(crec, &value);

    select_db(CHUNK_DB);

    redisReply *reply = redisCommand(redis, "SET %b %b", key.data, key.size, value.data, value.size);

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to SET chunk\n");
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_STATUS);

    free(value.data);
    freeReplyObject(reply);
}

int search_container(struct container_rec* r){

    int ret = 0;

    select_db(CONTAINER_DB);

    redisReply *reply = redisCommand(redis, "GET %b", &r->cid, sizeof(r->cid));

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to GET\n");
        exit(-1);
    }

    if(reply->type == REDIS_REPLY_NIL){
        ret = 0;
    }else{
        assert(reply->type == REDIS_REPLY_STRING);
        memcpy(r, reply->str, reply->len);
        ret = 1;
    }

    freeReplyObject(reply);

    return ret;
}

void update_container(struct container_rec* r){

    select_db(CONTAINER_DB);

    redisReply *reply = redisCommand(redis, "SET %b %b", &r->cid, sizeof(r->cid), r, sizeof(*r));

    if(reply == NULL){
        fprintf(stderr, "Cannot put container \n");
        exit(-1);
    }
    
    assert(reply->type == REDIS_REPLY_STATUS);

    freeReplyObject(reply);
}

int search_region(struct region_rec* r){

    int ret = 0;

    select_db(REGION_DB);

    redisReply *reply = redisCommand(redis, "GET %b", &r->rid, sizeof(r->rid));

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to GET\n");
        exit(-1);
    }

    if(reply->type == REDIS_REPLY_NIL){
        ret = 0;
    }else{
        assert(reply->type == REDIS_REPLY_STRING);
        memcpy(r, reply->str, reply->len);
        ret = 1;
    }

    freeReplyObject(reply);

    return ret;
}

void update_region(struct region_rec* r){

    select_db(REGION_DB);

    redisReply *reply = redisCommand(redis, "SET %b %b", &r->rid, sizeof(r->rid), r, sizeof(*r));

    if(reply == NULL){
        fprintf(stderr, "Cannot put region\n" );
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_STATUS);

    freeReplyObject(reply);
}

static void serial_file_rec(struct file_rec* r, KVOBJ* value){
    
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

static void unserial_file_rec(KVOBJ *value, struct file_rec *r){

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
    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = &r->fid;
    key.size = sizeof(r->fid);

    value.data = NULL;
    value.size = 0;

    select_db(FILE_DB);

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to GET\n");
        exit(-1);
    }

    int ret = 0;
    if(reply->type == REDIS_REPLY_NIL){
        ret = 0;
    }else{
        assert(reply->type == REDIS_REPLY_STRING);
        value.data = reply->str;
        value.size = reply->len;
        unserial_file_rec(&value, r);
        ret = 1;
    }

    freeReplyObject(reply);

    return ret;
}

void update_file(struct file_rec* r){
    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = &r->fid;
    key.size = sizeof(r->fid);

    serial_file_rec(r, &value);

    select_db(FILE_DB);

    redisReply *reply = redisCommand(redis, "SET %b %b", key.data, key.size, value.data, value.size);

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to SET\n");
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_STATUS);

    free(value.data);
    freeReplyObject(reply);

}

static int ITERATOR_DB = CHUNK_DB;
static redisReply *scan_reply = NULL;
static int remaining_replies = 0;

void init_iterator(char *type){
    if(strcmp(type, "CHUNK") == 0){
        ITERATOR_DB = CHUNK_DB;
    }else if(strcmp(type, "CONTAINER") == 0){
        ITERATOR_DB = CONTAINER_DB;
    }else if(strcmp(type, "REGION") == 0){
        ITERATOR_DB = REGION_DB;
    }else if(strcmp(type, "FILE") == 0){
        ITERATOR_DB = FILE_DB;
    }else{
        fprintf(stderr, "invalid iterator type!\n");
        exit(-1);
    }

    select_db(ITERATOR_DB);

    scan_reply = redisCommand(redis, "SCAN 0");
    assert(scan_reply->type == REDIS_REPLY_ARRAY);

    /* This is the length of scan_reply->element[1]->element[] */
    remaining_replies = scan_reply->element[1]->elements;

}

void close_iterator(){
    freeReplyObject(scan_reply);
}

/* return 1: no more data
 * return 0: more data */
/* dedup_fid = 1: remove duplicate file id in file list
 * dedup_fid = 0: do not remove */
int iterate_chunk(struct chunk_rec* r, int dedup_fid){

    if(scan_reply->element[0] == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more chunk\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        scan_reply = redisCommand(redis, "SCAN %d", scan_reply->element[0]);
        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;
    }

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies-1]->str;
    key.size = scan_reply->element[1]->element[remaining_replies-1]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    memcpy(r->hash, key.data, key.size);
    r->hashlen = key.size;

    unserial_chunk_rec(&value, r);

    if(dedup_fid && r->rcount > r->fcount){
        int* list = &r->list[r->lsize/2];
        int step = 0, i;
        for(i=1; i<r->rcount; i++){
            if(list[i] == list[i-1]){
                step++;
                continue;
            }
            list[i-step] = list[i];
        }

        assert(step == r->rcount - r->fcount);
    }

    freeReplyObject(reply);

    return 0;
}

int iterate_container(struct container_rec* r){
    if(scan_reply->element[0] == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more container\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        scan_reply = redisCommand(redis, "SCAN %d", scan_reply->element[0]);
        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;
    }

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies-1]->str;
    key.size = scan_reply->element[1]->element[remaining_replies-1]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    struct container_rec* v = value.data;
    r->cid = v->cid;
    r->psize = v->psize;
    r->lsize = v->lsize;

    freeReplyObject(reply);
    return 0;
}

int iterate_region(struct region_rec* r){

    if(scan_reply->element[0] == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more region\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        scan_reply = redisCommand(redis, "SCAN %d", scan_reply->element[0]);
        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;
    }

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies-1]->str;
    key.size = scan_reply->element[1]->element[remaining_replies-1]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    struct region_rec *v = value.data;
    r->rid = v->rid;
    r->psize = v->psize;
    r->lsize = v->lsize;

    freeReplyObject(reply);
    return 0;
}

int iterate_file(struct file_rec* r){

    if(scan_reply->element[0] == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more file\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        scan_reply = redisCommand(redis, "SCAN %d", scan_reply->element[0]);
        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;
    }

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies-1]->str;
    key.size = scan_reply->element[1]->element[remaining_replies-1]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    unserial_file_rec(&value, r);

    freeReplyObject(reply);
    return 0;
}
