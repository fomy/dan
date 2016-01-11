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


/*
 * Generate file system layout.
 * The forward layout: non-dedup and dedup
 * The backward layout: non-dedup and dedup
 */ 

/*
 * input: the input trace file
 * ouput: output file
 */
void generate_forward_nodedup_layout(char *input, char *output)
{
	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;

	int cf_id = -1;/* the current file id */
	int cf_cnum = 0;/* the chunk number */

	handle = hashfile_open(input);

	if (!handle) {
		fprintf(stderr, "Error opening hash file: %d!", errno);
		exit(-1);
	}

	int fd = open(output, O_CREAT|O_WRONLY, S_IWUSR|S_IRUSR);
	if(fd < 0){
		fprintf(stderr, "cannot open file %s\n", output);
		exit(-1);
	}

	char hash[20];
	memset(hash, 0, 20);
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

		GSequence *hashqueue = g_sequence_new(free);

		cf_id++;

		while (1) {
			ci = hashfile_next_chunk(handle);
			if (!ci) /* exit the loop if it was the last chunk */
				break;

			cf_cnum++;

			int hashsize = hashfile_hash_size(handle)/8;
			int chunksize = ci->size;
			memcpy(hash, ci->hash, hashsize);
			memcpy(&hash[hashsize], &chunksize, sizeof(chunksize));

			char* newhash = malloc(20 + sizeof(chunksize));
			memcpy(newhash, hash, 20);
			memcpy(newhash + 20, &chunksize, sizeof(chunksize));
			g_sequence_append(hashqueue, newhash);
		}

		if (cf_cnum == 0) {
			/* empty file excluded */
			cf_id--;
			g_sequence_free(hashqueue);
			continue;
		}

		write(fd, &cf_id, sizeof(cf_id));
		write(fd, &cf_cnum, sizeof(cf_cnum));

		GSequenceIter *iter = g_sequence_get_begin_iter(hashqueue);
		while (!g_sequence_iter_is_end(iter)) {
			write(fd, g_sequence_get(iter), 24);
			iter = g_sequence_iter_next(iter);
		}

		write(fd, &cf_id, sizeof(cf_id));

		cf_cnum = 0;
		g_sequence_free(hashqueue);
	}
	hashfile_close(handle);

	close(fd);
}

void generate_backward_nodedup_layout(char *input, char *output)
{
	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;

	int cf_id = -1;/* the current file id */
	int cf_cnum = 0;/* the chunk number */

	handle = hashfile_open(input);

	if (!handle) {
		fprintf(stderr, "Error opening hash file: %d!", errno);
		exit(-1);
	}

	GSequence *hashqueue = g_sequence_new(free);

	char hash[20];
	memset(hash, 0, 20);
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

		cf_id++;

		int *fid = malloc(sizeof(*fid));
		*fid = cf_id;
		g_sequence_prepend(hashqueue, fid);

		/*fprintf(stderr, "%s, %"PRIu64"\n", hashfile_curfile_path(handle), */
		/*hashfile_curfile_size(handle));*/
		while (1) {
			ci = hashfile_next_chunk(handle);
			if (!ci) /* exit the loop if it was the last chunk */
				break;

			cf_cnum++;

			int hashsize = hashfile_hash_size(handle)/8;
			int chunksize = ci->size;
			memcpy(hash, ci->hash, hashsize);
			memcpy(&hash[hashsize], &chunksize, sizeof(chunksize));

			char* newhash = malloc(20 + sizeof(chunksize));
			memcpy(newhash, hash, 20);
			memcpy(newhash + 20, &chunksize, sizeof(chunksize));
			g_sequence_prepend(hashqueue, newhash);
		}

		if (cf_cnum == 0) {
			/* empty file excluded */
			g_sequence_remove(g_sequence_get_begin_iter(hashqueue));
			cf_id--;
			continue;
		}

		char *fend = malloc(sizeof(int) + sizeof(int));
		memcpy(fend, &cf_id, sizeof(int));
		memcpy(fend + sizeof(int), &cf_cnum, sizeof(int));
		g_sequence_prepend(hashqueue, fend);

		cf_cnum = 0;
	}
	hashfile_close(handle);

	fprintf(stderr, "queue size = %d\n", g_sequence_get_length(hashqueue));

	int fd = open(output, O_CREAT|O_WRONLY, S_IWUSR|S_IRUSR);
	if (fd < 0) {
		fprintf(stderr, "cannot open file %s\n", reverse_file);
		exit(-1);
	}

	GSequenceIter *iter = g_sequence_get_begin_iter(hashqueue);
	while (!g_sequence_iter_is_end(iter)) {
		/* fend */
		char * fend = g_sequence_get(iter);
		/* id and chunk number */
		write(fd, fend, 8);
		memcpy(&cf_id, fend, sizeof(cf_id));
		memcpy(&cf_cnum, fend + 4, sizeof(cf_cnum));

		/* chunks */
		int i = 0;
		for (; i < cf_cnum; i++) {
			iter = g_sequence_iter_next(iter);
			write(fd, g_sequence_get(iter), 24);
		}

		/* fid */
		iter = g_sequence_iter_next(iter);
		int *tmpfid = g_sequence_get(iter);
		write(fd, tmpfid, sizeof(int));
		assert(memcmp(tmpfid, &cf_id, sizeof(int)) == 0);

		iter = g_sequence_iter_next(iter);
	}

	close(fd);
	g_sequence_free(hashqueue);
}

/* In dedup trace, there is no file indicator */
void generate_forward_dedup_layout(char *input, char *output)
{
}

void generate_backward_dedup_layout(char *input, char *output)
{
}

void generate_forward_dedup_defragmented_layout(char *input, char *output)
{
}

struct bin {
	int binfile_id; /* not NULL; >-1 */
	char minhash[20]; /* primary key */
	GQueue *hash_list;
}

static void free_bin(struct bin *b) 
{
	g_queue_free(b->hash_list);
	free(b);
}

static gboolean hash20_equal(gpointer a, gpointer b){
	return !memcmp(a, b, 20);
}

/* defragmented layout by the representative fingerprint */
void generate_similarity_based_layout(char *input, char *output)
{
	char buf[4096];
	struct hashfile_handle *handle;
	const struct chunk_info *ci;

	/* the fingerprint index to identify duplicate chunks */
	GHashTable *fp_index = g_hash_table_new_full(g_int_hash, hash20_equal,
			free, NULL);
	/* mapping minimal hashes to BIN files */
	GHashTable *bin_index = g_hash_table_new_full(g_int_hash,
			hash20_equal, NULL, free_bin);

	int bin_num = 0;

	handle = hashfile_open(input);

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

		/* a new file; init for it */
		/* unique_hashes: the list of unique hashes in this file */
		GQueue *unique_hashes = g_queue_new();
		/* the minimal hash of this file */
		char minhash[20];
		memset(minhash, 0xff, 20);

		char curhash[20];
		memset(curhash, 0, 20);

		int empty = 1;

		while (1) {
			ci = hashfile_next_chunk(handle);
			if (!ci) /* exit the loop if it was the last chunk */
				break;

			empty = 0;

			int hashsize = hashfile_hash_size(handle)/8;
			int chunksize = ci->size;
			memcpy(curhash, ci->hash, hashsize);
			memcpy(&curhash[hashsize], &chunksize, sizeof(chunksize));

			if (memcmp(curhash, minhash, 20) < 0) {
				memcpy(minhash, curhash, 20);
			}

			if (!g_hash_table_contains(fp_index, curhash)) {
				char *newhash = malloc(20);
				memcpy(newhash, curhash, 20);
				g_hash_table_insert(fp_index, newhash, NULL);
				g_queue_push_tail(unique_hashes, newhash);
			}
		}

		/* skip an empty file */
		if (empty) {
			g_queue_free(unique_hashes);
			continue;
		} 

		struct bin* b = g_hash_table_lookup(bin_index, minhash);

		if (!b) {
			b = malloc(sizeof(struct bin));
			b->binfile_id = bin_num++;
			memcpy(b->minhash, minhash, 20);
			b->hash_list = g_queue_new();

			g_hash_table_insert(bin_index, &b->minhash, b);
		}

		char *hash_elem = NULL;
		while ((hash_elem = g_queue_pop_head(unique_hashes))) {
			g_queue_push_tail(b->hash_list, hash_elem);
		}

		assert(g_queue_get_length(unique_hashes) == 0);
		g_queue_free(unique_hashes);

		if (g_queue_get_length(b->hash_list) > 1000) {
			/* too long; flush the queue to disk */
			char fname[100];
			sprintf(fname, "bins/tmp%d", b->binfile_id);
			int bin_fd;
			if (access(fname, F_OK) == 0) {
				/* existed */
				bin_fd = open(fname, O_WRONLY|O_APPEND);
			} else {
				bin_fd = open(fname, O_CREAT|O_WRONLY|O_APPEND, 
						S_IRUSR|S_IWUSR);
			}
			assert(bin_fd >= 0);

			char *hash_elem = NULL;
			while ((hash_elem = g_queue_pop_head(b->hash_list))) {
				write(bin_fd, hash_elem, 20);
			}
			close(bin_fd);
		}
	}
	hashfile_close(handle);

	/* iterate the bin_index */
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, bin_index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct bin *b = value;
		assert(b->binfile_id >= 0 && b->hash_list != NULL);
		assert(memcmp(b->minhash, key, 20) == 0);

		char fname[100];
		sprintf(fname, "bins/tmp%d", b->binfile_id);
		int bin_fd;
		if (access(fname, F_OK) == 0) {
			/* existed */
			bin_fd = open(fname, O_WRONLY|O_APPEND);
		} else {
			bin_fd = open(fname, O_CREAT|O_WRONLY|O_APPEND, S_IRUSR|S_IWUSR);
		}
		assert(bin_fd >= 0);

		char *hash_elem = NULL;
		while ((hash_elem = g_queue_pop_head(b->hash_list))) {
			write(bin_fd, hash_elem, 20);
		}

		close(bin_fd);
	}

	g_hash_table_destroy(fp_index);
	g_hash_table_destroy(bin_index);

	int of = open(output, O_CREAT|O_WRONLY, S_IWUSR|S_IRUSR);
	int i = 0;
	for (; i < bin_num; i++) {
		char fname[100];
		sprintf(fname, "bins/tmp%d", i);
		int bin_fd = open(fname, O_RDONLY);

		char hash[20];
		while (read(bin_fd, hash, 20) == 20) {
			write(of, hash, 20);
		}

		close(bin_fd);
		unlink(fname);
	}
	close(of);
}

int main(int argc, char *argv[])
{
	char *input = NULL, *output = NULL;
	int opt = 0;
	while ((opt = getopt_long(argc, argv, "i:o:", NULL, NULL))
			!= -1) {
		switch (opt) {
			case'i':
				input = optarg;
				break;
			case 'o':
				output = optarg;
				break;
			default:
				fprintf(stderr, "invalid option\n");
				return -1;
		}
	}

	assert(input && output);

	generate_similarity_based_layout(input, output);

	return 0;
}
