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
#include <glib.h>


/* Use this macros if libhashfile library is installed on your system */
// #include <libhashfile.h>

/* Use this macros if libhashfile library is NOT installed on your system */
#include "data.h"
#include "libhashfile.h"

#define MAXLINE	4096

struct chunk_item {
    int size;
    char hash[20];
    char minhash[20];
    char suffix[8];
};

static GHashTable* chunkset = NULL;

static gboolean hash_equal(gpointer a, gpointer b){
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

static int read_hashfile(char *hashfile_name)
{
    char buf[MAXLINE];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;
    time_t scan_start_time;
    int ret;

    handle = hashfile_open(hashfile_name);

    int total_chunks = 0;
    int collisions = 0;
    int detected_collisions = 0;
    int chunks_read_back = 0;

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

        char minhash[20];
        memset(minhash, 0xff, 20);
        char suffix[8];

        parse_file_suffix(hashfile_curfile_path(handle), suffix, 8);

        GHashTable *curfile = g_hash_table_new_full(g_int64_hash, hash_equal, NULL, free);

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;

            struct chunk_item *chunk = malloc(sizeof(struct chunk_item));
            memset(chunk, 0, sizeof(*chunk));

            chunk->size = ci->size;
            memcpy(chunk->hash, ci->hash, hashfile_hash_size(handle)/8);

            if(memcmp(chunk->hash, minhash, 20) < 0){
                memcpy(minhash, chunk->hash, 20);
            }

            struct chunk_item* target = g_hash_table_lookup(curfile, chunk->hash);
            if(target){
                if(target->size != chunk->size){
                    printf("+Find an intra-file collision! Cannot be detected!\n");
                    collisions++;
                }
                free(chunk);
            }else{
                g_hash_table_insert(curfile, chunk->hash, chunk);
            }

            total_chunks++;
        }

        GHashTableIter iter;
        gpointer key, value;
        g_hash_table_iter_init(&iter, curfile);

        while(g_hash_table_iter_next(&iter, &key, &value)){
            struct chunk_item* chunk = value;
            struct chunk_item* target = g_hash_table_lookup(chunkset, key);
            if(target){
                if(target->size != chunk->size){
                    printf("+Find a collision! Try to detect it!\n");
                    collisions++;
                }

                if(memcmp(target->minhash, minhash, 20) != 0 && strcmp(target->suffix, suffix) != 0){
                    chunks_read_back++;
                    if(target->size != chunk->size){
                        printf("-Collision Detected!\n");
                        detected_collisions++;
                    }
                }
            }else{
                memcpy(chunk->minhash, minhash, 20);
                memcpy(chunk->suffix, suffix, 8);
                g_hash_table_iter_steal(&iter);
                g_hash_table_insert(chunkset, chunk->hash, chunk);
            }
        }

        g_hash_table_destroy(curfile);

    }

    hashfile_close(handle);

    g_hash_table_destroy(chunkset);

    printf("# of chunks read back: %d; %.4f of total chunks\n", chunks_read_back, 1.0*chunks_read_back/total_chunks);
    printf("# of hash collisions: %d; %d detected\n", collisions, detected_collisions);
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Wrong usage!\n");
        fprintf(stderr, "Usage: %s <hashfile>\n", argv[0]);
        return -1;
    }

    int ret = read_hashfile(argv[1]);

    return ret;
}
