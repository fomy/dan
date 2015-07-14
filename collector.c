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

/* Use this macros if libhashfile library is installed on your system */
// #include <libhashfile.h>

/* Use this macros if libhashfile library is NOT installed on your system */
#include "data.h"
#include "store.h"
#include "libhashfile.h"

#define MAXLINE	4096

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

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));
    chunk.lsize = 2;
    chunk.list = malloc(sizeof(int) * chunk.lsize);
    struct container_rec container;
    memset(&container, 0, sizeof(container));
    struct region_rec region;
    memset(&region, 0, sizeof(region));
    struct file_rec file;

    /* statistics for generating IDs
     * ID starts from 0 */
    int chunk_count = 0;
    int container_count = 0;
    int region_count = 0;
    int file_count = 0;

    int dup_count = 0;

    handle = hashfile_open(hashfile_name);

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

        /* file start */
        memset(&file, 0, sizeof(file));
        memset(&file.minhash, 0xff, sizeof(file.minhash));
        file.fid = file_count;

        char* fname = parse_file_name(hashfile_curfile_path(handle));
        file.fname = malloc(strlen(fname)+1);
        strcpy(file.fname, fname);

        MD5_CTX ctx;
        MD5_Init(&ctx); 

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;

            memcpy(chunk.hash, ci->hash, hashfile_hash_size(handle)/8);
            chunk.hashlen = hashfile_hash_size(handle)/8;

            MD5_Update(&ctx, chunk.hash, chunk.hashlen);

            if(memcmp(chunk.hash, file.minhash, chunk.hashlen) < 0){
                memcpy(file.minhash, chunk.hash, chunk.hashlen);
            }

            if(memcmp(chunk.hash, file.maxhash, chunk.hashlen) > 0){
                memcpy(file.maxhash, chunk.hash, chunk.hashlen);
            }
            /* We find a hash collision */
            /*if(memcmp(chunk.hash, col, sizeof(col)) == 0){*/
                /*print_chunk_hash(chunk_count, chunk.hash, hashfile_hash_size(handle)/8);*/
                /*printf("chunk size: %d\n", ci->size);*/
            /*}*/

            ret = search_chunk(&chunk);
            if(ret == 0){
                /* A unique chunk */
                chunk.csize = ci->size;
                chunk.cratio = ci->cratio;

                /* TO-DO: write to the open region */
                while(add_chunk_to_region(&chunk, &region) != 1){
                    /* the last region is full, write it to the open container */
                    add_region_to_container(&region, &container);
                    update_region(&region);
                    region_count++;

                    /* open a new region */
                    reset_region_rec(&region);
                    region.rid = region_count;

                    if(container_full(&container)){
                        update_container(&container);
                        container_count++;

                        reset_container_rec(&container);
                        container.cid = container_count;
                    }
                }

                chunk.rid = region.rid;
                chunk.cid = container.cid;

            }else if(ret == 1){
                /* A duplicate chunk */
                /*printf("duplicate, %d\n", chunk.csize);*/
                dup_count++;

                if(chunk.csize != ci->size){
                    print_chunk_hash(chunk_count, chunk.hash, hashfile_hash_size(handle)/8);
                    printf("Hash Collision: %d to %llu\n", chunk.csize, ci->size);
                    /*assert(chunk.csize == ci->size);*/
                }
                /*assert(chunk.cratio == ci->cratio);*/

                /* TO-DO: update the associated container and region records. */
                if(chunk.rid == region.rid){
                    /* The chunk is in the open region */
                    assert(chunk.cid == container.cid);

                    container.lsize += chunk.csize;
                    region.lsize += chunk.csize;
                
                }else if(chunk.cid == container.cid){
                    struct region_rec target_region;
                    target_region.rid = chunk.rid;
                    ret = search_region(&target_region);
                    if(ret != 1){
                        fprintf(stderr, "Cannot find the target region\n");
                        exit(2);
                    }
                    target_region.lsize += chunk.csize;
                    update_region(&target_region);

                    container.lsize += chunk.csize;
                }else{
                    struct container_rec target_container;
                    target_container.cid = chunk.cid;
                    ret = search_container(&target_container);
                    if(ret != 1){
                        fprintf(stderr, "Cannot find the target container\n");
                        exit(2);
                    }
                    target_container.lsize += chunk.csize;
                    update_container(&target_container);

                    struct region_rec target_region;
                    target_region.rid = chunk.rid;
                    ret = search_region(&target_region);
                    if(ret != 1){
                        fprintf(stderr, "Cannot find the target region\n");
                        exit(2);
                    }
                    target_region.lsize += chunk.csize;
                    update_region(&target_region);
                }

            }else{
                exit(2);
            }

            assert(chunk.lsize >= (chunk.rcount + 1) * 2);

            /* TO-DO: update the chunk.list */
            /* determine whether we need to update file list */
            if(!check_file_list(&chunk.list[chunk.lsize/2], chunk.rcount, file_count))
                chunk.fcount++;

            chunk.list[chunk.rcount] = chunk_count;
            chunk.list[chunk.lsize/2 + chunk.rcount] = file_count;
            chunk.rcount++;

            ret = update_chunk(&chunk);

            /* update file info */
            file.cnum++;
            file.fsize += chunk.csize;

            chunk_count++;
        }

        MD5_Final(file.hash, &ctx);

        /* file end; update it */
        update_file(&file);
        free(file.fname);
        file_count++;
    }

    hashfile_close(handle);

    printf("%d duplicate chunks out of %d\n", dup_count, chunk_count);
    printf("%d files\n", file_count);
    return 0;
}

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

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Wrong usage!\n");
        fprintf(stderr, "Usage: %s <hashfile>\n", argv[0]);
        return -1;
    }

    char *buf = malloc(strlen(argv[1]) + 1);
    strcpy(buf, argv[1]);
    char *env_name = get_env_name(buf);

    int ret = create_database(env_name);
    free(buf);
    if(ret != 0){
        return ret;
    }

    ret = read_hashfile(argv[1]);

    close_database();

    return ret;
}
