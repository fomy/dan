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
