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

static int read_hashfile(char *argv[], int argc)
{
	char buf[MAXLINE];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;
	int ret;

	struct chunk_rec chunk;
	memset(&chunk, 0, sizeof(chunk));
	chunk.listsize = 1;
	chunk.list = malloc(sizeof(int) * chunk.listsize);
	struct container_rec container;
	memset(&container, 0, sizeof(container));
	struct region_rec region;
	memset(&region, 0, sizeof(region));
	struct file_rec file;

	int64_t syssize = 0;
	int64_t dupsize = 0;

	/* statistics for generating IDs
	 * ID starts from 0 */
	int chunk_count = 0;
	int container_count = 0;
	int region_count = 0;
	int file_count = 0; /* excluding empty files */
	int empty_files = 0;

	int dup_count = 0;

	file_count = get_file_number();

	int i = 0;
	for (; i<argc; i++) {
		char *hashfile_name = argv[i];
		handle = hashfile_open(hashfile_name);

		if (!handle) {
			fprintf(stderr, "Error opening hash file: %d!", errno);
			return -1;
		}

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
			printf("%d:%s, %"PRIu64"\n", 
					file.fid, file.fname, hashfile_curfile_size(handle));

			MD5_CTX ctx;
			MD5_Init(&ctx); 

			while (1) {
				ci = hashfile_next_chunk(handle);
				if (!ci) /* exit the loop if it was the last chunk */
					break;

				int hashsize = hashfile_hash_size(handle)/8;
				int chunksize = ci->size;
				memcpy(chunk.hash, ci->hash, hashsize);
				memcpy(&chunk.hash[hashsize], &chunksize, sizeof(chunksize));
				chunk.hashlen = 20;
				/*chunk.hashlen = hashfile_hash_size(handle)/8 + sizeof(chunksize);*/

				MD5_Update(&ctx, chunk.hash, chunk.hashlen);

				if(memcmp(chunk.hash, file.minhash, chunk.hashlen) < 0){
					memcpy(file.minhash, chunk.hash, chunk.hashlen);
				}

				if(memcmp(chunk.hash, file.maxhash, chunk.hashlen) > 0){
					memcpy(file.maxhash, chunk.hash, chunk.hashlen);
				}

				ret = search_chunk(&chunk);
				if(ret == STORE_NOTFOUND){
					/* A unique chunk */
					chunk.csize = ci->size;
					chunk.cratio = ci->cratio;

					/* TO-DO: write to the open region */
					while(add_chunk_to_region(&chunk, &region) != 1){
						/* the last region is full, write it to the open container */
						add_region_to_container(&region, &container);
						region_count++;

						/* open a new region */
						reset_region_rec(&region);
						region.rid = region_count;

						if(container_full(&container)){
							container_count++;

							reset_container_rec(&container);
							container.cid = container_count;
						}
					}

					chunk.rid = region.rid;
					chunk.cid = container.cid;

					chunk.rcount = 1;
					chunk.elem_num = 1;
					chunk.list[0] = file.fid;

					insert_chunk(&chunk);

				}else if(ret == STORE_EXISTED){
					/* A duplicate chunk */
					/*printf("duplicate, %d\n", chunk.csize);*/
					dup_count++;
					dupsize += chunk.csize;

					if(chunk.csize != ci->size){
						print_chunk_hash(chunk_count, chunk.hash, 
								hashfile_hash_size(handle)/8);
						printf("Hash Collision: %d to %llu\n", 
								chunk.csize, ci->size);
						/*assert(chunk.csize == ci->size);*/
					}
					/*assert(chunk.cratio == ci->cratio);*/
					reference_chunk(&chunk, file.fid);
				}else{
					exit(2);
				}
				syssize += chunk.csize;


				/* update file info */
				file.cnum++;
				file.fsize += chunk.csize;

				chunk_count++;
			}

			MD5_Final(file.hash, &ctx);

			if(file.fsize != hashfile_curfile_size(handle))
				printf("%"PRId64" != %"PRIu64"\n", file.fsize, hashfile_curfile_size(handle));
			/* file end; update it */
			if(file.fsize > 0){
				insert_file(&file);
				file_count++;
				/*assert(hashfile_curfile_size(handle) == file.fsize);*/
			}else{
				empty_files++;
			}
			free(file.fname);
			file.fname = NULL;
		}

		hashfile_close(handle);
	}

	printf("%.2fGB bytes in total, eliminating %.2fGB bytes, %.5f, %.5f\n", 
			1.0*syssize/1024/1024/1024, 
			1.0*dupsize/1024/1024/1024, 
			1.0*dupsize/syssize, 1.0*syssize/(syssize-dupsize));
	printf("%d duplicate chunks out of %d\n", dup_count, chunk_count);
	printf("%d files, excluding %d empty files\n", file_count, empty_files);
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
	if (argc < 2) {
		fprintf(stderr, "Wrong usage!\n");
		fprintf(stderr, "Usage: %s <hashfile>\n", argv[0]);
		return -1;
	}

	open_database("dbhome/");

	int ret = read_hashfile(&argv[1], argc - 1);

	close_database();

	return ret;
}
