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
#include <getopt.h>

/* Use this macros if libhashfile library is installed on your system */
// #include <libhashfile.h>

/* Use this macros if libhashfile library is NOT installed on your system */
#include "data.h"
#include "store.h"
#include "libhashfile.h"

#define MAXLINE	4096

#define PHYSICAL_LOCALITY 1
#define LOGICAL_LOCALITY 2

/* two chunks with this hash have different chunk size */
/* "18:06:bd:7a:61:11" */
char col[] = {0x18,0x6,0xbd,0x7a,0x61,0x11};

static int export_locality(char *hashfile_name, int locality)
{
    char buf[MAXLINE];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;
    time_t scan_start_time;
    int ret;

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));

    /* statistics for generating IDs
     * ID starts from 0 */
    int chunk_count = 0;
    int dup_count = 0;

    handle = hashfile_open(hashfile_name);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        return -1;
    }

    /* Print some information about the hash file */
    scan_start_time = hashfile_start_time(handle);
    /*printf("Collected at [%s] on %s",*/
            /*hashfile_sysid(handle),*/
            /*ctime(&scan_start_time));*/

    ret = hashfile_chunking_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized chunking method: %d!", errno);
        return -1;
    }

    /*printf("Chunking method: %s", buf);*/

    ret = hashfile_hashing_method_str(handle, buf, MAXLINE);
    if (ret < 0) {
        fprintf(stderr, "Unrecognized hashing method: %d!", errno);
        return -1;
    }

    /*printf("Hashing method: %s\n", buf);*/

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

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;

            int hashsize = hashfile_hash_size(handle)/8;
            int chunksize = ci->size;
            memcpy(chunk.hash, ci->hash, hashsize);
            memcpy(&chunk.hash[hashsize], &chunksize, sizeof(chunksize));
            chunk.hashlen = hashfile_hash_size(handle)/8 + sizeof(chunksize);

            if(search_chunk(&chunk) != 1){
                fprintf(stderr, "Cannot find the chunk\n");
                exit(-1);
            }

            if(locality == PHYSICAL_LOCALITY && chunk_count != chunk.list[0]){
                /*printf("%d > %d\n", chunk_count, chunk.list[0]);*/
                assert(chunk_count > chunk.list[0]);
            }else
                fprintf(stdout, "%d\n", chunk.rcount);

            chunk_count++;
        }
    }

    hashfile_close(handle);

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
    int opt = 0;
    int locality = LOGICAL_LOCALITY;

	while ((opt = getopt_long(argc, argv, "pl", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'p':
                locality = PHYSICAL_LOCALITY;
                break;
            case 'l':
                locality = LOGICAL_LOCALITY;
                break;
            default:
                return -1;
        }
    }

    open_database();

    int ret = export_locality(argv[optind], locality);

    close_database();

    return ret;
}
