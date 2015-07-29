/*
 * Copyright (c) 2014 Sonam Mandal
 * Copyright (c) 2014 Vasily Tarasov
 * Copyright (c) 2014 Will Buik
 * Copyright (c) 2014 Erez Zadok
 * Copyright (c) 2014 Geoff Kuenning
 * Copyright (c) 2014 Stony Brook University
 * Copyright (c) 2014 Harvey Mudd College
 * Copyright (c) 2014 The Research Foundation of the State University of New York
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <openssl/md5.h>
#include <getopt.h>
#include <glib.h>


/* Use this macros if libhashfile library is installed on your system */
// #include <libhashfile.h>

/* Use this macros if libhashfile library is NOT installed on your system */
#include "data.h"
#include "libhashfile.h"

#define MAXLINE	4096

struct chunk_item {
    int size;
    int fid;
    int sid;
    int rc;
    int64_t fsize;
    char hash[20];
    char minhash[20];
    char suffix[8];
};

static int segment_size = 0;
static int max_segment_size = 0;
static int min_segment_size = 0;

static GHashTable* chunkset = NULL;
static int file_count = 0;
static int segment_count = 0;
static int dup_chunks = 0;
static int collisions = 0;
static int detected_collisions = 0;
static int chunks_read_back = 0;
static char minhash[20];
static char suffix[8];

static gboolean hash_equal(gpointer a, gpointer b){
    return !memcmp(a, b, 6);
}

static gboolean minhash_equal(gpointer a, gpointer b){
    return !memcmp(a, b, 20);
}

static void print_chunk_hash(uint64_t chunk_count, const uint8_t *hash,
        int hash_size_in_bytes)
{
    int j;

    printf("Chunk %06"PRIu64 ": ", chunk_count);

    printf("%.2hhx", hash[0]);
    for (j = 1; j < hash_size_in_bytes; j++)
        printf(":%.2hhx", hash[j]);
    printf("\n");
}

/* two chunks with this hash have different chunk size */
/* "18:06:bd:7a:61:11" */
char col[] = {0x18,0x6,0xbd,0x7a,0x61,0x11};

static char* parse_file_name(char *path){
    int i = strlen(path) - 1;
    while(i>=0 && path[i]!='\\' && path[i]!='/')
        i--;
    return &path[i+1];
}

static void check_curfile(GHashTable* curfile){

    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, curfile);

    while(g_hash_table_iter_next(&iter, &key, &value)){
        struct chunk_item* chunk = value;
        struct chunk_item* target = g_hash_table_lookup(chunkset, key);
        if(target){
            if(memcmp(target->hash, chunk->hash, 20)!=0){
                fprintf(stderr, "+Find a collision between %s <-> %s! Try to detect it!\n", target->suffix, suffix);
                fprintf(stderr, "+Details: MinHash = %s, Size %lld <-> %lld\n", 
                        memcmp(target->minhash, chunk->minhash, 20) == 0?"True":"False",
                        target->fsize, chunk->fsize);
                collisions++;
            }else{
                dup_chunks++;
            }

            int read_back = 0;
            if(memcmp(target->minhash, minhash, 20) != 0){
                read_back = 1;
            } 
            if(read_back){
                chunks_read_back++;
                if(memcmp(target->hash, chunk->hash, 20)!=0){
                    fprintf(stderr, "-Collision Detected!\n");
                    detected_collisions++;
                }
            }
            target->rc++;
        }else{
            chunk->fid = file_count;
            memcpy(chunk->minhash, minhash, 20);
            memcpy(chunk->suffix, suffix, 8);
            g_hash_table_iter_steal(&iter);
            g_hash_table_insert(chunkset, chunk->hash, chunk);
        }

    }
}

static void check_cursegment(GHashTable* cursegment){

    GHashTableIter iter;
    gpointer key, value;

    GHashTable *whitelist = g_hash_table_new_full(g_int_hash, g_int_equal, free, NULL);

    g_hash_table_iter_init(&iter, cursegment);
    while(g_hash_table_iter_next(&iter, &key, &value)){
        struct chunk_item* chunk = value;
        struct chunk_item* target = g_hash_table_lookup(chunkset, key);
        if(target){
            int *sc = g_hash_table_lookup(whitelist, &target->sid);
            if(!sc){
                sc = malloc(sizeof(int)*2);
                sc[0] = target->sid;
                sc[1] = 0;
                g_hash_table_insert(whitelist, sc, sc);
            }
            sc[1]++;
        }
    }

    g_hash_table_iter_init(&iter, cursegment);
    while(g_hash_table_iter_next(&iter, &key, &value)){
        struct chunk_item* chunk = value;
        struct chunk_item* target = g_hash_table_lookup(chunkset, key);
        if(target){
            if(memcmp(target->hash, chunk->hash, 20)!=0){
                fprintf(stderr, "+Find a collision between %s <-> %s! Try to detect it!\n", target->suffix, suffix);
                fprintf(stderr, "+Details: MinHash = %s, Size %lld <-> %lld\n", 
                        memcmp(target->minhash, chunk->minhash, 20) == 0?"True":"False",
                        target->fsize, chunk->fsize);
                collisions++;
            }else{
                dup_chunks++;
            }

            int read_back = 0;
            if(memcmp(target->minhash, minhash, 20) != 0){
                int *sc = g_hash_table_lookup(whitelist, &target->sid);
                assert(sc);
                assert(sc[1]>0);
                if(sc[1] <= 1){
                    read_back = 1;
                }
            } 

            if(read_back){
                chunks_read_back++;
                if(memcmp(target->hash, chunk->hash, 20)!=0){
                    fprintf(stderr, "-Collision Detected!\n");
                    detected_collisions++;
                }
            }
            target->rc++;
        }else{
            chunk->fid = file_count;
            chunk->sid = segment_count;
            memcpy(chunk->minhash, minhash, 20);
            memcpy(chunk->suffix, suffix, 8);
            g_hash_table_iter_steal(&iter);
            g_hash_table_insert(chunkset, chunk->hash, chunk);
        }
    }

    g_hash_table_destroy(whitelist);
}

static int detect_by_segment_minhash(char *hashfile_name)
{
    char buf[MAXLINE];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;
    time_t scan_start_time;
    int ret;

    handle = hashfile_open(hashfile_name);

    int total_chunks = 0;

    chunkset = g_hash_table_new_full(g_int64_hash, hash_equal, NULL, free);

    GHashTable* minhashset = g_hash_table_new_full(g_int64_hash, minhash_equal, free, NULL);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        return -1;
    }

    /* Print some information about the hash file */
    scan_start_time = hashfile_start_time(handle);
    printf("Collected at [%s] on %s",
            hashfile_sysid(handle),
            ctime(&scan_start_time));

    ret = hashfile_chunking_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized chunking method: %d!", errno);
        return -1;
    }

    printf("Chunking method: %s", buf);

    ret = hashfile_hashing_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized hashing method: %d!", errno);
        return -1;
    }

    printf("Hashing method: %s\n", buf);

    /* Go over the files in a hashfile */
    while (1) {
        ret = hashfile_next_file(handle);
        if (ret < 0) {
            fprintf(stderr,
                    "Cannot get next file from a hashfile: %d!\n",
                    errno);
            return -1;
        }

        /* exit the loop if it was the last file */
        if (ret == 0)
            break;

        memset(minhash, 0xff, 20);

        parse_file_suffix(hashfile_curfile_path(handle), suffix, 8);
        if(strncmp(suffix, "edu,", 4) == 0){
            strcpy(suffix, "edu,?");
        }else if(strlen(suffix) == 0){
            strcpy(suffix, ".None");
        }

        GHashTable *cursegment = g_hash_table_new_full(g_int64_hash, hash_equal, NULL, free);

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;

            struct chunk_item *chunk = malloc(sizeof(struct chunk_item));
            memset(chunk, 0, sizeof(*chunk));

            chunk->size = ci->size;
            memcpy(chunk->hash, ci->hash, hashfile_hash_size(handle)/8);
            memcpy(chunk->hash+hashfile_hash_size(handle)/8, &chunk->size, sizeof(chunk->size));

            chunk->rc = 1;

            chunk->fsize = hashfile_curfile_size(handle);

            if(memcmp(chunk->hash, minhash, 20) < 0){
                memcpy(minhash, chunk->hash, 20);
            }

            struct chunk_item* target = g_hash_table_lookup(cursegment, chunk->hash);
            if(target){
                if(memcmp(chunk->hash, target->hash, 20)!=0){
                    fprintf(stderr, "+Find an intra-file collision! Cannot be detected! File size = %lld, Type = %s\n", 
                            hashfile_curfile_size(handle), suffix);
                    collisions++;
                }
                free(chunk);
                dup_chunks++;
            }else{
                g_hash_table_insert(cursegment, chunk->hash, chunk);
            }

            if(g_hash_table_size(cursegment) > min_segment_size){
                int bits;
                memcpy(&bits, chunk->hash, 4);
                if((bits & (segment_size-1)) == 0 || g_hash_table_size(cursegment) > max_segment_size){
                    /* A new segment */
                    if(!g_hash_table_contains(minhashset, minhash)){
                        char* new_minhash = malloc(sizeof(minhash));
                        memcpy(new_minhash, minhash, sizeof(minhash));
                        g_hash_table_insert(minhashset, new_minhash, new_minhash);
                    }

                    check_cursegment(cursegment);
                    g_hash_table_remove_all(cursegment);
                    memset(minhash, 0xff, 20);
                    segment_count++;
                }
            }

            total_chunks++;
        }

        if(!g_hash_table_contains(minhashset, minhash)){
            char* new_minhash = malloc(sizeof(minhash));
            memcpy(new_minhash, minhash, sizeof(minhash));
            g_hash_table_insert(minhashset, new_minhash, new_minhash);
        }
        check_cursegment(cursegment);
        g_hash_table_destroy(cursegment);
        file_count++;
        segment_count++;

    }

    hashfile_close(handle);

    g_hash_table_destroy(chunkset);

    fprintf(stderr, "# of chunks read back: %d; %.4f of total chunks, %.4f of dup chunks; %.4f D/R\n", 
            chunks_read_back, 1.0*chunks_read_back/total_chunks,
            1.0*chunks_read_back/dup_chunks, 1.0*dup_chunks/total_chunks);
    fprintf(stderr, "# of hash collisions: %d; %d detected\n", collisions, detected_collisions);
    fprintf(stderr, "# of bins: %d\n", g_hash_table_size(minhashset));

    g_hash_table_destroy(minhashset);
    return 0;
}

static int detect_by_file_minhash(char *hashfile_name)
{
    char buf[MAXLINE];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;
    time_t scan_start_time;
    int ret;

    handle = hashfile_open(hashfile_name);

    int total_chunks = 0;

    chunkset = g_hash_table_new_full(g_int64_hash, hash_equal, NULL, free);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        return -1;
    }

    /* Print some information about the hash file */
    scan_start_time = hashfile_start_time(handle);
    printf("Collected at [%s] on %s",
            hashfile_sysid(handle),
            ctime(&scan_start_time));

    ret = hashfile_chunking_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized chunking method: %d!", errno);
        return -1;
    }

    printf("Chunking method: %s", buf);

    ret = hashfile_hashing_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized hashing method: %d!", errno);
        return -1;
    }

    printf("Hashing method: %s\n", buf);

    /* Go over the files in a hashfile */
    while (1) {
        ret = hashfile_next_file(handle);
        if (ret < 0) {
            fprintf(stderr,
                    "Cannot get next file from a hashfile: %d!\n",
                    errno);
            return -1;
        }

        /* exit the loop if it was the last file */
        if (ret == 0)
            break;

        memset(minhash, 0xff, 20);

        parse_file_suffix(hashfile_curfile_path(handle), suffix, 8);
        if(strncmp(suffix, "edu,", 4) == 0){
            strcpy(suffix, "edu,?");
        }else if(strlen(suffix) == 0){
            strcpy(suffix, ".None");
        }

        GHashTable *curfile = g_hash_table_new_full(g_int64_hash, hash_equal, NULL, free);

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;

            struct chunk_item *chunk = malloc(sizeof(struct chunk_item));
            memset(chunk, 0, sizeof(*chunk));

            chunk->size = ci->size;
            memcpy(chunk->hash, ci->hash, hashfile_hash_size(handle)/8);
            memcpy(chunk->hash+hashfile_hash_size(handle)/8, &chunk->size, sizeof(chunk->size));

            chunk->rc = 1;

            chunk->fsize = hashfile_curfile_size(handle);

            if(memcmp(chunk->hash, minhash, 20) < 0){
                memcpy(minhash, chunk->hash, 20);
            }

            struct chunk_item* target = g_hash_table_lookup(curfile, chunk->hash);
            if(target){
                if(target->size != chunk->size){
                    fprintf(stderr, "+Find an intra-file collision! Cannot be detected! File size = %lld, Type = %s\n", 
                            hashfile_curfile_size(handle), suffix);
                    collisions++;
                }
                free(chunk);
                dup_chunks++;
            }else{
                g_hash_table_insert(curfile, chunk->hash, chunk);
            }

            total_chunks++;
        }

        check_curfile(curfile);
        g_hash_table_destroy(curfile);
        file_count++;

    }

    hashfile_close(handle);

    g_hash_table_destroy(chunkset);

    fprintf(stderr, "# of chunks read back: %d; %.4f of total chunks, %.4f of dup chunks\n", 
            chunks_read_back, 1.0*chunks_read_back/total_chunks,
            1.0*chunks_read_back/dup_chunks);
    fprintf(stderr, "# of hash collisions: %d; %d detected\n", collisions, detected_collisions);
    return 0;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    int segment = 0;
    while ((opt = getopt_long(argc, argv, "s:", NULL, NULL))
            != -1) {
        switch (opt) {
            case 's':
                segment = 1;
                segment_size = atoi(optarg);
                min_segment_size = segment_size/4;
                max_segment_size = segment_size*4;
                break;
            default:
                return -1;
        }
    }

    int ret;
    if(segment == 0)
        ret = detect_by_file_minhash(argv[optind]);
    else
        ret = detect_by_segment_minhash(argv[optind]);

    return ret;
}
