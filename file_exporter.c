#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include <inttypes.h>
#include <assert.h>
#include "store.h"

struct file_list {
	/* file hash or min hash */
	char hash[20];
	GList *head;
};

struct file_item {
	int64_t fsize;
	int fid;
	char hash[20];
	char fname[0];
};

static gboolean only_one_item(gpointer key, gpointer value, gpointer user_data){
	struct file_list* fl = value;
	return g_list_next(fl->head) == NULL;
}

static void free_file_list(struct file_list* list){
	g_list_free_full(list->head, free);
	free(list);
}

static gboolean hash_equal(gconstpointer a, gconstpointer b){
	return memcmp(a, b, 20) == 0;
}

static gboolean file_equal(gconstpointer a, gconstpointer b){
	struct file_item* fa = a;
	struct file_item* fb = b;
	return hash_equal(fa->hash, fb->hash);
}

static void print_hash(const uint8_t *hash,
		int hash_size_in_bytes)
{
	int j;
	printf("%.2hhx", hash[0]);
	for (j = 1; j < hash_size_in_bytes; j++)
		printf(":%.2hhx", hash[j]);
	printf("\n");
}

struct intra_redundant_file{
	int64_t fsize;
	char suffix[8];
	int fid;
	/* number of chunks eliminated by intra-file deduplication */
	int cnum;
	/* size of chunks eliminated by intra_file deduplication */
	int64_t csize;
	char minhash[20];
};

/* intra-file redundancy */
void collect_intra_redundant_files()
{
	init_iterator("CHUNK");

	struct chunk_rec r;
	memset(&r, 0, sizeof(r));
	struct file_rec f;
	memset(&f, 0, sizeof(f));

	int count = 0;
	GHashTable *fileset = g_hash_table_new_full(g_int_hash, 
			g_int_equal, NULL, free);

	while (iterate_chunk(&r) == ITER_CONTINUE) {

		if (r.rcount > r.fcount) {
			int i = 1;
			for (; i < r.elem_num; i++) {
				if (r.list[i] < 0) {
					/* An intra-file redundancy */
					struct intra_redundant_file * irfile = 
						g_hash_table_lookup(fileset, &r.list[i-1]);
					if (irfile == NULL) {
						f.fid = r.list[i-1];
						int ret = search_file(&f);
						assert(ret == STORE_EXISTED);

						irfile = malloc(sizeof(struct intra_redundant_file));
						irfile->fid = f.fid;
						irfile->fsize = f.fsize;
						memcpy(irfile->minhash, f.minhash, sizeof(f.minhash));
						parse_file_suffix(f.fname, irfile->suffix, 
								sizeof(irfile->suffix));
						if (strncmp(irfile->suffix, "edu,", 4) == 0) {
							strcpy(irfile->suffix, "edu,?");
						} else if (strlen(irfile->suffix) == 0) {
							strcpy(irfile->suffix, ".None");
						}
						irfile->cnum = 0;
						irfile->csize = 0;

						g_hash_table_insert(fileset, &irfile->fid, irfile);
					}

					irfile->cnum -= r.list[i];
					int64_t chunksize = r.csize;
					chunksize *= r.list[i];
					irfile->csize -= chunksize;
				}
			}
		}
	}

	close_iterator("CHUNK");

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, fileset);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct intra_redundant_file *irfile = value;
		printf("FILE %d %" PRId64 " %s %d %" PRId64 " ", 
				irfile->fid, irfile->fsize, irfile->suffix,
				irfile->cnum, irfile->csize);
		print_hash(irfile->minhash, 10);
	}

	fprintf(stderr, "%d intra-redundant files\n", g_hash_table_size(fileset));

	g_hash_table_destroy(fileset);

}

/* sharing some chunks but with different hash/minhash */
void collect_missed_files()
{
	init_iterator("CHUNK");

	struct chunk_rec r;
	memset(&r, 0, sizeof(r));

	int count = 0;
	while (iterate_chunk(&r) == ITER_CONTINUE) {

		if (r.rcount > 1 && r.fcount > 1) {
			struct file_rec *files[r.fcount];
			int i = 0, j = 0;
			for (; i < r.elem_num; i++) {
				if (r.list[i] < 0) continue;
				files[j] = malloc(sizeof(struct file_rec));
				memset(files[j], 0, sizeof(struct file_rec));
				files[j]->fid = r.list[i];
				search_file(files[j++]);
			}
			assert(j == r.fcount);

			for (i = 1; i < r.fcount; i++) {
				if (memcmp(files[i]->minhash, files[i-1]->minhash, 
							sizeof(files[i]->minhash)) != 0) {
					char suffix[8];
					printf("CHUK %d ", r.fcount);
					print_hash(r.hash, 10);
					int j = 0;
					for(; j<r.fcount; j++){
						parse_file_suffix(files[j]->fname, suffix, sizeof(suffix));
						if(strncmp(suffix, "edu,", 4) == 0){
							strcpy(suffix, "edu,?");
						}else if(strlen(suffix) == 0){
							strcpy(suffix, ".None");
						}
						printf("FILE %d %" PRId64 " %s %s ", files[j]->fid, 
								files[j]->fsize, files[j]->fname, suffix);
						print_hash(files[j]->minhash, 10);
					}

					count++;
					break;
				}
			}

			for (i = 0; i < r.fcount; i++) {
				free_file_rec(files[i]);
			}
		}
	}
	fprintf(stderr, "%d chunks are shared between missed files\n", count);

	close_iterator("CHUNK");

}

static int all_files_are_identical(struct file_list* fl){
	GList* cur = fl->head;
	GList* next = NULL;
	while((next = g_list_next(cur))){
		if(!file_equal(cur->data, next->data))
			return 0;
		cur = next;
	}
	return 1;
}

int collect_similar_files()
{
	init_iterator("FILE");

	struct file_rec r;
	memset(&r, 0, sizeof(r));

	GHashTable* hashset = g_hash_table_new_full(g_int_hash, hash_equal, 
			NULL, free_file_list);

	while (iterate_file(&r) == ITER_CONTINUE) {
		if (r.fsize > 0) {
			struct file_list* fl = g_hash_table_lookup(hashset, r.minhash);
			if (fl == NULL) {
				fl = malloc(sizeof(struct file_list));
				fl->head = NULL;
				memcpy(fl->hash, r.minhash, sizeof(r.minhash));
				g_hash_table_insert(hashset, fl->hash, fl);
			}

			struct file_item* item = malloc(sizeof(*item) 
					+ strlen(r.fname) + 1);
			item->fid = r.fid;
			item->fsize = r.fsize;
			memcpy(item->hash, r.hash, sizeof(r.hash));
			strcpy(item->fname, r.fname);
			fl->head = g_list_prepend(fl->head, item);
		} else {
			fprintf(stderr, "empty files should have been excluded\n");
			exit(-1);
		}
	}

	close_iterator("FILE");

	fprintf(stderr, "totally %d bins\n", g_hash_table_size(hashset));
	g_hash_table_foreach_remove(hashset, only_one_item, NULL);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, hashset);
	char suffix[8];
	int all_dupfiles_count = 0;
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct file_list* fl = value;
		if (all_files_are_identical(fl)) {
			/* excluding the bins of all identical files */
			all_dupfiles_count++;
			continue;
		}

		printf("HASH %d ", g_list_length(fl->head));
		print_hash(fl->hash, 10);
		GList* elem = g_list_first(fl->head);
		do {
			struct file_item* item = elem->data;
			parse_file_suffix(item->fname, suffix, sizeof(suffix));
			if (strncmp(suffix, "edu,", 4) == 0) {
				strcpy(suffix, "edu,?");
			} else if (strlen(suffix) == 0) {
				strcpy(suffix, ".None");
			}
			printf("FILE %d %" PRId64 " %s %s ", item->fid, item->fsize,
					item->fname, suffix);
			print_hash(item->hash, 10);
		} while ((elem = g_list_next(elem)));
	}

	fprintf(stderr, "%d bins (size > 1), from %d of which all files are identical\n",
			g_hash_table_size(hashset), all_dupfiles_count);
	g_hash_table_destroy(hashset);

	return 0;
}

int collect_duplicate_files()
{
	init_iterator("FILE");

	struct file_rec r;
	memset(&r, 0, sizeof(r));

	GHashTable* hashset = g_hash_table_new_full(g_int_hash, hash_equal, NULL, 
			free_file_list);

	while (iterate_file(&r) == ITER_CONTINUE) {
		if (r.fsize > 0) {
			struct file_list* fl = g_hash_table_lookup(hashset, r.hash);
			if (fl == NULL) {
				fl = malloc(sizeof(struct file_list));
				fl->head = NULL;
				memcpy(fl->hash, r.hash, sizeof(r.hash));
				g_hash_table_insert(hashset, fl->hash, fl);
			}

			struct file_item* item = malloc(sizeof(*item) 
					+ strlen(r.fname) + 1);
			item->fid = r.fid;
			item->fsize = r.fsize;
			strcpy(item->fname, r.fname);
			fl->head = g_list_prepend(fl->head, item);
		} else {
			fprintf(stderr, "empty files should have been excluded\n");
			exit(-1);
		}
	}

	close_iterator("FILE");

	g_hash_table_foreach_remove(hashset, only_one_item, NULL);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, hashset);
	char suffix[8];
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct file_list* fl = value;
		printf("HASH %d ", g_list_length(fl->head));
		print_hash(fl->hash, 10);
		GList* elem = g_list_first(fl->head);
		do {
			struct file_item* item = elem->data;
			parse_file_suffix(item->fname, suffix, sizeof(suffix));
			if (strncmp(suffix, "edu,", 4) == 0 ) {
				strcpy(suffix, "edu,?");
			} else if (strlen(suffix) == 0) {
				strcpy(suffix, ".None");
			}
			printf("FILE %d %" PRId64 " %s %s\n", item->fid, item->fsize,
					item->fname, suffix);
		} while ((elem = g_list_next(elem)));
	}

	g_hash_table_destroy(hashset);

	return 0;
}

#define FLAG_DUPLICATE_FILES 1
#define FLAG_SIMILAR_FILES 2
#define FLAG_MISSED_FILES 3
#define FLAG_INTRA_REDUNDANT_FILES 4

int main(int argc, char *argv[])
{
	int opt = 0;
	int flag = FLAG_DUPLICATE_FILES;
	while ((opt = getopt_long(argc, argv, "dsmi", NULL, NULL))
			!= -1) {
		switch (opt) {
			case 'd':
				flag = FLAG_DUPLICATE_FILES;
				break;
			case 's':
				flag = FLAG_SIMILAR_FILES;
				break;
			case 'm':
				flag = FLAG_MISSED_FILES;
				break;
			case 'i':
				flag = FLAG_INTRA_REDUNDANT_FILES;
				break;
			default:
				return -1;
		}
	}

	open_database("dbhome/");

	if(flag == FLAG_DUPLICATE_FILES)
		collect_duplicate_files();
	else if(flag == FLAG_SIMILAR_FILES)
		collect_similar_files();
	else if(flag == FLAG_MISSED_FILES)
		collect_missed_files();
	else if(flag == FLAG_INTRA_REDUNDANT_FILES)
		collect_intra_redundant_files();

	close_database();

	return 0;
}
