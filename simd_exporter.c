#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>
#include <glib.h>
#include <fcntl.h>
#include "libhashfile.h"
#include "store.h"

static gboolean hash20_equal(gpointer a, gpointer b){
	return !memcmp(a, b, 20);
}


/* The number of lines relies on chunksize;
 * Only chunk-level trace requires this  */
static void print_a_chunk(int chunksize, int64_t content){
	int lines_no = (chunksize+1023)/1024;
	assert(lines_no >= 1);
	assert(lines_no <= 16);
	int i = 0;
	for(; i<lines_no; i++)
		printf("%"PRId64"\n", content);
}

/* there is no trace for chunk-level with dedup */
/* Fixed-sized file system block of 8 KB if weighted */
void chunk_nodedup_simd_trace(char **path, int count,  int weighted){
	if (weighted) {
		fprintf(stderr, "CHUNK:NO DEDUP:WEIGHTED\n");
		printf("CHUNK:NO DEDUP:WEIGHTED\n");
		init_iterator("CHUNK");

		struct chunk_rec chunk;
		memset(&chunk, 0, sizeof(chunk));

		int64_t sum = 0;
		int64_t count = 0;
		/* USE part */
		while(iterate_chunk(&chunk, 0) == 0){

			int i = 0;
			int64_t size = chunk.csize;
			for(; i<chunk.rcount; i++){
				int lines_no = (chunk.csize+1023)/1024;
				sum += size*lines_no;
				count += lines_no;
			}
		}

		fprintf(stderr, "%.6f\n", 1.0*sum/count);

		close_iterator();
	}else{
		fprintf(stderr, "CHUNK:NO DEDUP:NOT WEIGHTED\n");
		printf("CHUNK:NO DEDUP:NOT WEIGHTED\n");
	}

	fprintf(stderr, "No trace for this model; only for test\n");
}

/*  
 * Chunk level, without file semantics 
 * Dedup
 * (no trace for chunk-level no-dedup model)
 */
void chunk_dedup_simd_trace(char **path, int count, int weighted, char *pophashfile)
{
	if (weighted) {
		fprintf(stderr, "CHUNK:DEDUP:WEIGHTED\n");
		printf("CHUNK:DEDUP:WEIGHTED\n");
	} else {
		fprintf(stderr, "CHUNK:DEDUP:NOT WEIGHTED\n");
		printf("CHUNK:DEDUP:NOT WEIGHTED\n");
	}

	init_iterator("CHUNK");

	struct chunk_rec chunk;
	memset(&chunk, 0, sizeof(chunk));

	int64_t psize = 0;
	int64_t lsize = 0;
	int64_t total_chunks = 0;
	/* USE part */
	int64_t sum4mean = 0;
	int64_t count4mean = 0;
	while (iterate_chunk(&chunk, 0) == 0) {

		int64_t sum = chunk.csize;
		sum *= chunk.rcount;

		lsize += sum;
		psize += chunk.csize;

		total_chunks += chunk.rcount;

		if (weighted) {
			sum4mean += sum * chunk.csize;
			count4mean += chunk.csize;
			print_a_chunk(chunk.csize, sum);
		} else {
			sum4mean += sum; 
			count4mean += chunk.csize;
			print_a_chunk(chunk.csize, chunk.rcount);
		}
	}

	printf("%.6f\n", 1.0*lsize/psize);
	fprintf(stderr, "D/F = %.4f, total_chunks = %"PRId64"\n", 1.0*lsize/psize, 
			total_chunks);
	fprintf(stderr, "mean = %.4f, per DF = %.6f\n", 1.0*sum4mean/count4mean, 
			1.0*sum4mean*psize/count4mean/lsize);

	close_iterator();

	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;

	int64_t restore_logical_bytes = 0;
	int64_t restore_physical_bytes = 0;
	int64_t restore_chunks = 0;
	GHashTable* chunks = g_hash_table_new_full(g_int_hash, hash20_equal, free, 
			NULL);

	/* RAID Failure part */
	/* 1 - 99 */
	int step = 1;
	/* All chunks lost */
	puts("0");

	if (pophashfile) {
		int popfd = open(pophashfile, O_RDONLY);
		char pophashbuf[20];
		while (read(popfd, pophashbuf, 20) == 20) {
			char *pophash = malloc(20);
			memcpy(pophash, pophashbuf, 20);

			/* restoring a pop chunk */
			memcpy(chunk.hash, pophash, 20);
			assert(search_chunk(&chunk));

			int64_t sum = chunk.csize;
			sum *= chunk.rcount;
			restore_chunks += chunk.rcount;

			restore_physical_bytes += chunk.csize;
			restore_logical_bytes += sum;

			int progress = restore_physical_bytes * 100/psize;
			while (progress >= step && step <= 99) {
				if (weighted) {
					printf("%.6f\n", 1.0*restore_logical_bytes/lsize);
					fprintf(stderr, "%.6f\n", 1.0*restore_logical_bytes/lsize);
				} else {
					printf("%.6f\n", 1.0*restore_chunks/total_chunks);
					fprintf(stderr, "%.6f\n", 1.0*restore_chunks/total_chunks);
				}
				step++;
			}

			assert(!g_hash_table_contains(chunks, pophash));
			g_hash_table_insert(chunks, pophash, NULL);
		}
		close(popfd);
	}

	int pc = 0;
	for (; pc < count; pc++) {
		handle = hashfile_open(path[pc]);

		if (!handle) {
			fprintf(stderr, "Error opening hash file: %d!", errno);
			exit(-1);
		}


		while (1) {
			int ret = hashfile_next_file(handle);
			if (ret < 0) {
				fprintf(stderr,
						"Cannot get next file from a hashfile: %d!\n",
						errno);
				exit(-1);
			}
			if (ret == 0)
				break;

			while (1) {
				ci = hashfile_next_chunk(handle);
				if (!ci) /* exit the loop if it was the last chunk */
					break;

				int hashsize = hashfile_hash_size(handle)/8;
				int chunksize = ci->size;
				memcpy(chunk.hash, ci->hash, hashsize);
				memcpy(&chunk.hash[hashsize], &chunksize, sizeof(chunksize));
				chunk.hashlen = hashfile_hash_size(handle)/8 + sizeof(chunksize);

				if (!g_hash_table_contains(chunks, chunk.hash)) {
					assert(search_chunk(&chunk));
					int64_t sum = chunk.csize;
					sum *= chunk.rcount;
					restore_chunks += chunk.rcount;

					restore_physical_bytes += chunk.csize;
					restore_logical_bytes += sum;

					int progress = restore_physical_bytes * 100/psize;
					while (progress >= step && step <= 99) {
						if (weighted) {
							printf("%.6f\n", 1.0*restore_logical_bytes/lsize);
							fprintf(stderr, "%.6f\n", 1.0*restore_logical_bytes/lsize);
						} else {
							printf("%.6f\n", 1.0*restore_chunks/total_chunks);
							fprintf(stderr, "%.6f\n", 1.0*restore_chunks/total_chunks);
						}
						step++;
					}
					char* hash = malloc(20);
					memcpy(hash, chunk.hash, 20);
					g_hash_table_insert(chunks, hash, NULL);
				}
			}
		}
		hashfile_close(handle);
	}
	g_hash_table_destroy(chunks);

	puts("1.0");
}

/*
 * File level, no dedup
 * weighted by size?
 */
void file_nodedup_simd_trace(char **path, int count,  int weighted)
{
	if (weighted) {
		printf("FILE:NO DEDUP:WEIGHTED\n");
		fprintf(stderr, "FILE:NO DEDUP:WEIGHTED\n");
	} else {
		printf("FILE:NO DEDUP:NOT WEIGHTED\n");
		fprintf(stderr, "FILE:NO DEDUP:NOT WEIGHTED\n");
	}

	int64_t sys_capacity = 0;
	int64_t sys_file_number = 0;

	init_iterator("CHUNK");

	struct chunk_rec chunk;
	memset(&chunk, 0, sizeof(chunk));
	struct file_rec fr;
	memset(&fr, 0, sizeof(fr));

	/* USE part */
	while (iterate_chunk(&chunk, 0) == 0) {

		int64_t sum = chunk.csize;
		sum *= chunk.rcount;
		sys_capacity += sum;
		int i = 0;
		int prev = -1;
		for (; i<chunk.rcount; i++) {
			int fid = chunk.list[chunk.rcount+i];
			fr.fid = fid;
			search_file(&fr);

			prev = fid;
			if(weighted)
				printf("%"PRId64"\n", fr.fsize);
			else{
				/* A single file is lost */
				/* no need to output */
			}
		}
	}

	close_iterator();

	sys_file_number = get_file_number();

	fprintf(stderr, "capacity = %.4f GB, Files = %"PRId64"\n", 
			1.0*sys_capacity/1024/1024/1024, sys_file_number);

	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;
	/* RAID Failure part */
	/* All files lost */
	puts("0");

	int64_t restore_bytes = 0;
	int64_t restore_files = 0;
	int64_t restore_file_bytes = 0;

	/* 1 - 99 */
	int step = 1;

	int pc = 0;
	for (; pc < count; pc++) {
		handle = hashfile_open(path[pc]);

		if (!handle) {
			fprintf(stderr, "Error opening hash file: %d!", errno);
			exit(-1);
		}

		while (1) {
			int ret = hashfile_next_file(handle);
			if (ret < 0) {
				fprintf(stderr,
						"Cannot get next file from a hashfile: %d!\n",
						errno);
				exit(-1);
			}
			if (ret == 0)
				break;

			int64_t filesize = 0;
			while (1) {
				ci = hashfile_next_chunk(handle);
				if (!ci) /* exit the loop if it was the last chunk */
					break;

				int progress = restore_bytes * 100 / sys_capacity;
				while(progress >= step && step <= 99){
					if(!weighted)
						printf("%.6f\n", 1.0*restore_files/sys_file_number);
					else
						printf("%.6f\n", 1.0*restore_file_bytes/sys_capacity);
					step++;
				}

				/* It will overflow */
				/*restore_bytes += ci->size;*/
				int size = ci->size;
				restore_bytes += size;
				filesize += size;
			}

			/*if(filesize != hashfile_curfile_size(handle))*/
			/*printf("%"PRId64" is not %"PRIu64"\n", filesize, hashfile_curfile_size(handle));*/
			/*else*/
			/*printf("%"PRId64" == %"PRIu64"\n", filesize, hashfile_curfile_size(handle));*/
			if(filesize == 0)
				continue;

			restore_files++;
			restore_file_bytes += filesize;

		}

		hashfile_close(handle);
	}
	puts("1.0");
}

struct restoring_file{
	int id;
	int chunk_num;
	int64_t size;
};

void file_dedup_simd_trace(char** path, int count,  int weighted, char *pophashfile)
{
	if (weighted) {
		printf("FILE:DEDUP:WEIGHTED\n");
		fprintf(stderr, "FILE:DEDUP:WEIGHTED\n");
	} else {
		printf("FILE:DEDUP:NOT WEIGHTED\n");
		fprintf(stderr, "FILE:DEDUP:NOT WEIGHTED\n");
	}

	init_iterator("CHUNK");

	struct chunk_rec chunk;
	memset(&chunk, 0, sizeof(chunk));
	struct file_rec fr;
	memset(&fr, 0, sizeof(fr));

	/* USE part */
	int64_t psize = 0;
	int64_t lsize = 0;
	while (iterate_chunk(&chunk, 0) == 0) {

		int64_t sum = chunk.csize;
		sum *= chunk.rcount;
		lsize += sum;
		psize += chunk.csize;
		if (!weighted) {
			printf("%d\n", chunk.fcount);
		} else {
			int i = 0;
			int prev = -1;
			int64_t sum = 0;
			for (; i<chunk.rcount; i++) {
				int fid = chunk.list[chunk.rcount+i];
				if (fid == prev)
					continue;
				fr.fid = fid;
				search_file(&fr);

				sum+=fr.fsize;
				prev = fid;
			}
			printf("%"PRId64"\n", sum);
		}
	}

	printf("%.6f\n", 1.0*lsize/psize);
	fprintf(stderr, "LS = %.4f GB, PS = %.4f GB, D/F = %.4f\n", 
			1.0*lsize/1024/1024/1024,
			1.0*psize/1024/1024/1024, 1.0*lsize/psize);

	close_iterator();

	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;

	int64_t sys_file_number = get_file_number();

	/* All files lost */
	puts("0");

	int64_t restore_bytes = 0;
	int64_t restore_files = 0;
	int64_t restore_file_bytes = 0;

	/* RAID Failure part */
	/* 1 - 99 */
	int step = 1;

	GHashTable* files = g_hash_table_new_full(g_int_hash, g_int_equal, 
			NULL, free);
	GHashTable* chunks = g_hash_table_new_full(g_int_hash, hash20_equal, 
			free, NULL);

	if (pophashfile) {
		int popfd = open(pophashfile, O_RDONLY);
		char pophashbuf[20];
		while (read(popfd, pophashbuf, 20) == 20) {
			char *pophash = malloc(20);
			memcpy(pophash, pophashbuf, 20);

			/* restoring a pop chunk */
			memcpy(chunk.hash, pophash, 20);
			assert(search_chunk(&chunk));

			int i = 0;
			for (;i < chunk.rcount; i++) {
				int fid = chunk.list[chunk.rcount + i];
				struct restoring_file* rfile = g_hash_table_lookup(files, &fid);
				if (!rfile) {
					fr.fid = fid;
					search_file(&fr);

					rfile = malloc(sizeof(*rfile));

					rfile->id = fid;
					rfile->chunk_num = fr.cnum;
					rfile->size = fr.fsize;

					g_hash_table_insert(files, &rfile->id, rfile);
				}
				rfile->chunk_num--;

				if (rfile->chunk_num == 0) {
					/* a file is restored */
					/*fprintf(stderr, "complete file %d\n", fid);*/
					restore_files++;
					restore_file_bytes += rfile->size;
				}
				assert(rfile->chunk_num >= 0);
			}

			restore_bytes += chunk.csize;
			int progress = restore_bytes * 100 / psize;
			while (progress >= step && step <= 99) {
				if (!weighted)
					printf("%.6f\n", 1.0*restore_files/sys_file_number);
				else
					printf("%.6f\n", 1.0*restore_file_bytes/lsize);
				step++;
			}

			assert(!g_hash_table_contains(chunks, pophash));
			g_hash_table_insert(chunks, pophash, NULL);
		}
		close(popfd);
	}

	int pc = 0;
	for (; pc < count; pc++) {

		handle = hashfile_open(path[pc]);

		if (!handle) {
			fprintf(stderr, "Error opening hash file: %d!", errno);
			exit(-1);
		}

		while (1) {
			int ret = hashfile_next_file(handle);
			if (ret < 0) {
				fprintf(stderr,
						"Cannot get next file from a hashfile: %d!\n",
						errno);
				exit(-1);
			}
			if (ret == 0)
				break;

			while (1) {
				ci = hashfile_next_chunk(handle);
				if (!ci) /* exit the loop if it was the last chunk */
					break;

				int hashsize = hashfile_hash_size(handle)/8;
				int chunksize = ci->size;
				memcpy(chunk.hash, ci->hash, hashsize);
				memcpy(&chunk.hash[hashsize], &chunksize, sizeof(chunksize));
				chunk.hashlen = hashfile_hash_size(handle)/8 + sizeof(chunksize);

				if (!g_hash_table_contains(chunks, chunk.hash)) {
					/* restore a chunk */
					assert(search_chunk(&chunk));
					int i = 0;
					for (; i < chunk.rcount; i++) {
						int fid = chunk.list[chunk.rcount + i];
						struct restoring_file* rfile = 
							g_hash_table_lookup(files, &fid);
						if (!rfile) {
							fr.fid = fid;
							search_file(&fr);

							rfile = malloc(sizeof(*rfile));

							rfile->id = fid;
							rfile->chunk_num = fr.cnum;
							rfile->size = fr.fsize;

							g_hash_table_insert(files, &rfile->id, rfile);
						}
						rfile->chunk_num--;

						if(rfile->chunk_num == 0){
							/* a file is restored */
							/*fprintf(stderr, "complete file %d\n", fid);*/
							restore_files++;
							restore_file_bytes += rfile->size;
						}
						assert(rfile->chunk_num >= 0);
					}

					restore_bytes += chunk.csize;
					int progress = restore_bytes * 100/psize;
					while (progress >= step && step <= 99) {
						if (!weighted)
							printf("%.6f\n", 1.0*restore_files/sys_file_number);
						else
							printf("%.6f\n", 1.0*restore_file_bytes/lsize);
						step++;
					}
					char* hash = malloc(20);
					memcpy(hash, chunk.hash, 20);
					g_hash_table_insert(chunks, hash, hash);
				}
			}
		}

		hashfile_close(handle);
	}
	puts("1.0");

	g_hash_table_destroy(files);
	g_hash_table_destroy(chunks);
	fprintf(stderr, "restore %.4f GB\n", 1.0*restore_file_bytes/1024/1024/1024);

}

int main(int argc, char *argv[])
{
	/* dedup? */
	int dedup = 0;
	/* weighted by file/chunk size? */
	int weighted = 0;
	/* chunk level or file level */
	int file_level = 0;

	char *pophash = NULL;

	int opt = 0;
	while ((opt = getopt_long(argc, argv, "fwdp:", NULL, NULL))
			!= -1) {
		switch (opt) {
			case'f':
				file_level = 1;
				break;
			case 'd':
				dedup = 1;
				break;
			case 'w':
				weighted = 1;
				break;
			case 'p':
				pophash = optarg;
				break;
			default:
				fprintf(stderr, "invalid option\n");
				return -1;
		}
	}

	open_database();

	if (!file_level) {
		if (!dedup)
			chunk_nodedup_simd_trace(&argv[optind], argc - optind,  weighted);
		else
			chunk_dedup_simd_trace(&argv[optind], argc - optind, weighted, pophash);
	} else {
		if (!dedup)
			file_nodedup_simd_trace(&argv[optind], argc - optind, weighted);
		else
			file_dedup_simd_trace(&argv[optind], argc - optind, weighted, pophash);
	}
	close_database();

	return 0;
}
