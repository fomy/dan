#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <errno.h>
#include <hiredis/hiredis.h>
#include <glib.h>

#include "data.h"

typedef struct {
    void* data;
    size_t size;
} KVOBJ;

#define CHUNK_DB 1
#define FILE_DB 2

static redisContext *redis = NULL;
static int current_db = -1;
static GHashTable *chunkdb_cache = NULL;
/* For search_file */
static GHashTable *filedb_cache = NULL;

static void select_db(int db){
    if(current_db != db){
        redisReply *reply = redisCommand(redis, "SELECT %d", db);
        assert(reply->type == REDIS_REPLY_STATUS);
        current_db = db;
        freeReplyObject(reply);
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
    memcpy(value->data + off, &r->list[r->list_size/2], sizeof(int) * r->rcount);
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
        r->list = malloc(sizeof(int) * r->rcount * 2);
    }else{
        r->list = realloc(r->list, sizeof(int) * r->rcount * 2);
    }

    memcpy(r->list, value->data + len, sizeof(int) * r->rcount);
    len += sizeof(int) * r->rcount;
    memcpy(&r->list[r->rcount], value->data + len, sizeof(int) * r->rcount);
    len += sizeof(int) * r->rcount;

    assert(len == value->size);
}

void open_database(){
    redis = redisConnect("127.0.0.1", 6379);
    if(redis != NULL && redis->err){
        fprintf(stderr, "Error: %s\n", redis->errstr);
        exit(-1);
    }
    filedb_cache = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, free_file_rec);
}

static gboolean chunk_equal(struct chunk_rec* a, struct chunk_rec* b){
    return memcmp(a->hash, b->hash, a->hashlen) == 0;
}

void create_database(){
    chunkdb_cache = g_hash_table_new_full(g_int64_hash, chunk_equal, NULL, free_chunk_rec);
    open_database();
}

static gboolean send_kv(gpointer k, gpointer v, gpointer data){
    struct chunk_rec *r = v;
    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = r->hash;
    key.size = r->hashlen;

    serial_chunk_rec(r, &value);

    select_db(CHUNK_DB);

    redisReply *reply = redisCommand(redis, "SET %b %b", key.data, key.size, value.data, value.size);

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to SET chunk\n");
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_STATUS);

    free(value.data);
    freeReplyObject(reply);

    return TRUE;
}

void close_database(){
    if(chunkdb_cache){

        g_hash_table_foreach_remove(chunkdb_cache, send_kv, NULL);

        /*redisReply *reply = redisCommand(redis, "SAVE");*/
        /*assert(reply->type == REDIS_REPLY_STATUS);*/
        /*freeReplyObject(reply);*/
        /*g_hash_table_destroy(chunkdb_cache);*/
        /*chunkdb_cache = NULL;*/
    }
    redisFree(redis);
}

/* 
 * Search hash in database.
 * chunk_rec returns a pointer if exists; otherwise, returns NULL
 * return 1 indicates existence; 0 indicates not exist.
 * */
int search_chunk_local(struct chunk_rec *r){

    assert(chunkdb_cache);

    struct chunk_rec *chunk = g_hash_table_lookup(chunkdb_cache, r);
    if(chunk == NULL){
        reset_chunk_rec(r);
        return 0;
    }
    r->cid = chunk->cid;
    r->rid = chunk->rid;
    r->cratio = chunk->cratio;
    r->csize = chunk->csize;
    r->fcount = chunk->fcount;
    r->rcount = chunk->rcount;

    return 1;
}

int search_chunk(struct chunk_rec *r){
    KVOBJ key, value;

    key.data = r->hash;
    key.size = r->hashlen;

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
        reset_chunk_rec(r);
        ret = 0;
    }else{
        assert(reply->type == REDIS_REPLY_STRING);
        value.data = reply->str;
        value.size = reply->len;
        unserial_chunk_rec(&value, r);
        ret = 1;
    }

    freeReplyObject(reply);

    return ret;
}

void update_chunk(struct chunk_rec *r){

    assert(chunkdb_cache != NULL);

    struct chunk_rec *chunk = g_hash_table_lookup(chunkdb_cache, r);
    if(chunk == NULL){
        chunk = malloc(sizeof(struct chunk_rec));
        memcpy(chunk->hash, r->hash, sizeof(chunk->hash));
        chunk->cid = r->cid;
        chunk->rid = r->rid;
        chunk->cratio = r->cratio;
        chunk->csize = r->csize;
        chunk->hashlen = r->hashlen;
        chunk->fcount = 1;
        chunk->rcount = 1;
        chunk->list_size = 2;
        chunk->list = malloc(sizeof(int) * 2);
        memcpy(chunk->list, r->list, sizeof(int) * 2);
        g_hash_table_insert(chunkdb_cache, chunk, chunk);
    }else{

        chunk->rcount++;
        assert(chunk->rcount > 1);
        if(chunk->list_size < chunk->rcount * 2){
            int* oldlist = chunk->list;
            int oldlist_size = chunk->list_size;
            if(chunk->rcount == 2 || chunk->rcount == 3){
                chunk->list_size = chunk->rcount * 2;
                chunk->list = malloc(sizeof(int) * chunk->list_size);
            } else if (chunk->list_size < 65536) {
                chunk->list_size = chunk->list_size * 2;
                assert(chunk->list_size > chunk->rcount * 2);
                chunk->list = malloc(sizeof(int) * chunk->list_size);
                assert(chunk->list != NULL);
            } else {
                chunk->list_size += 65536;
                chunk->list = malloc(sizeof(int) * chunk->list_size);
                assert(chunk->list != NULL);
            }
            memcpy(chunk->list, oldlist, (chunk->rcount - 1) * sizeof(int));
            memcpy(&chunk->list[chunk->list_size/2], &oldlist[oldlist_size/2], (chunk->rcount - 1) * sizeof(int));
            free(oldlist);
        }

        chunk->list[chunk->rcount - 1] = r->list[0];
        chunk->list[chunk->list_size/2 + chunk->rcount - 1] = r->list[1];

        /*determine whether we need to update file list*/
        if(chunk->list[chunk->list_size/2 + chunk->rcount - 2 ] != r->list[1])
            chunk->fcount++;
    }

}

int64_t get_chunk_number(){
    select_db(CHUNK_DB);

    redisReply *reply = redisCommand(redis, "DBSIZE");

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to DBSIZE\n");
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_INTEGER);

    int64_t number = reply->integer;

    freeReplyObject(reply);
    
    return number;
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

    int ret = 0;
    struct file_rec* cached_file = g_hash_table_lookup(filedb_cache, &r->fid);
    if(!cached_file){
        cached_file = malloc(sizeof(*cached_file));
        memset(cached_file, 0, sizeof(*cached_file));

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
            fprintf(stderr, "Error: Fail to GET file; %s \n", redis->errstr);
            exit(-1);
        }

        if(reply->type == REDIS_REPLY_NIL){
            ret = 0;
        }else{
            assert(reply->type == REDIS_REPLY_STRING);
            value.data = reply->str;
            value.size = reply->len;
            unserial_file_rec(&value, cached_file);
            ret = 1;
        }

        freeReplyObject(reply);

        g_hash_table_insert(filedb_cache, &cached_file->fid, cached_file);
    }

    assert(r->fid == cached_file->fid);
    r->cnum = cached_file->cnum;
    r->fsize = cached_file->fsize;
    memcpy(r->hash, cached_file->hash, sizeof(r->hash));
    memcpy(r->minhash, cached_file->minhash, sizeof(r->minhash));
    memcpy(r->maxhash, cached_file->maxhash, sizeof(r->maxhash));

    if(r->fname == NULL)
        r->fname = malloc(strlen(cached_file->fname)+1);
    else
        r->fname = realloc(r->fname, strlen(cached_file->fname)+1);

    strcpy(r->fname, cached_file->fname);

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

    if(strcmp(scan_reply->element[0]->str, "0") == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more chunk\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        redisReply *tmp = redisCommand(redis, "SCAN %s", scan_reply->element[0]->str);
        freeReplyObject(scan_reply);
        scan_reply = tmp;

        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;

        if(remaining_replies == 0){
            assert(strcmp(scan_reply->element[0]->str, "0") == 0);
            fprintf(stderr, "no more chunk, replying with 0\n");
            return 1;
        }
    }

    remaining_replies--;
    assert(remaining_replies >= 0);

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies]->str;
    key.size = scan_reply->element[1]->element[remaining_replies]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    memcpy(r->hash, key.data, key.size);
    r->hashlen = key.size;

    unserial_chunk_rec(&value, r);

    if(dedup_fid && r->rcount > r->fcount){
        int* list = &r->list[r->rcount];
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

int iterate_file(struct file_rec* r){

    if(strcmp(scan_reply->element[0]->str, "0") == 0 && remaining_replies ==  0){
        fprintf(stderr, "no more file\n");
        return 1;
    }

    select_db(ITERATOR_DB);

    if(remaining_replies == 0){

        redisReply *tmp = redisCommand(redis, "SCAN %s", scan_reply->element[0]->str);
        freeReplyObject(scan_reply);
        scan_reply = tmp;

        assert(scan_reply->type == REDIS_REPLY_ARRAY);

        /* This is the length of scan_reply->element[1]->element[] */
        remaining_replies = scan_reply->element[1]->elements;

        if(remaining_replies == 0){
            assert(strcmp(scan_reply->element[0]->str, "0") == 0);
            fprintf(stderr, "no more file, replying with 0\n");
            return 1;
        }
    }

    remaining_replies--;

    KVOBJ key, value;
    memset(&key, 0, sizeof(KVOBJ));
    memset(&value, 0, sizeof(KVOBJ));

    key.data = scan_reply->element[1]->element[remaining_replies]->str;
    key.size = scan_reply->element[1]->element[remaining_replies]->len;

    redisReply *reply = redisCommand(redis, "GET %b", key.data, key.size);
    assert(reply->type == REDIS_REPLY_STRING);

    value.data = reply->str;
    value.size = reply->len;

    unserial_file_rec(&value, r);

    freeReplyObject(reply);
    return 0;
}

int64_t get_file_number(){
    select_db(FILE_DB);

    redisReply *reply = redisCommand(redis, "DBSIZE");

    if(reply == NULL){
        fprintf(stderr, "Error: Fail to DBSIZE\n");
        exit(-1);
    }

    assert(reply->type == REDIS_REPLY_INTEGER);

    int64_t number = reply->integer;

    freeReplyObject(reply);

    return number;
}
