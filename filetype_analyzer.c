#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include <inttypes.h>
#include "store.h"

struct suffix{
    /* The number of files with this suffix */
    int64_t num;
    /* The size of files with this suffix */
    int64_t size;
    char suffix[8];
};

static gboolean suffix_equal(gconstpointer a, gconstpointer b){
    return strcmp(a, b) == 0;
}

#define FLAG_SIZE 1
#define FLAG_NUM 2

static gint suffix_num_cmp(struct suffix* a, struct suffix* b, gpointer data){
    return b->num > a->num ? 1:-1;
}

static gint suffix_size_cmp(struct suffix* a, struct suffix* b, gpointer data){
    return b->size > a->size ? 1:-1;
}

int get_top_filetypes(int top){
    int ret = init_iterator("FILE");

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    /* File system */
    int64_t fs_count = 0;
    int64_t fs_size = 0;

    GHashTable *table = g_hash_table_new_full(g_int_hash, suffix_equal, NULL, free);
    struct suffix *s = NULL;
    while(iterate_file(&r) == 0){
        if(strncmp(r.suffix, "edu,", 4) == 0){
            strcpy(r.suffix, "edu,?");
        }else if(strlen(r.suffix) == 0){
            strcpy(r.suffix, ".None");
        }
        if((s = g_hash_table_lookup(table, r.suffix)) == NULL){
            s = malloc(sizeof(struct suffix));
            memcpy(s->suffix, r.suffix, sizeof(s->suffix));
            s->num = 0;
            s->size = 0;
            g_hash_table_insert(table, s->suffix, s);
        }
        s->num++;
        s->size += r.fsize;

        fs_count++;
        fs_size += r.fsize;
    }
    close_iterator();

    fprintf(stderr, "totally %d suffix in %" PRId64 " files\n", g_hash_table_size(table), fs_count);

    GSequence* seq_num = g_sequence_new(NULL), *seq_size = g_sequence_new(NULL);

    GHashTableIter iter;
    g_hash_table_iter_init(&iter, table);
    gpointer key, value;
    while(g_hash_table_iter_next(&iter, &key, &value)){
        g_sequence_insert_sorted(seq_num, value, suffix_num_cmp, NULL);
        g_sequence_insert_sorted(seq_size, value, suffix_size_cmp, NULL);
        if(g_sequence_get_length(seq_num) > top){
            /* Remove the last (smallest) one */
            g_sequence_remove(g_sequence_iter_prev(g_sequence_get_end_iter(seq_num)));
            g_sequence_remove(g_sequence_iter_prev(g_sequence_get_end_iter(seq_size)));
        }
    }

    int i = 0;
    GSequenceIter* num_iter = g_sequence_get_begin_iter(seq_num);
    GSequenceIter* size_iter = g_sequence_get_begin_iter(seq_size);
    for(; i<top; i++){
        struct suffix* num_suffix = g_sequence_get(num_iter);
        struct suffix* size_suffix = g_sequence_get(size_iter);
        printf("%8s %.4f %8s %.4f\n", num_suffix->suffix, 1.0*num_suffix->num/fs_count, size_suffix->suffix, 1.0*size_suffix->size/fs_size);
        num_iter = g_sequence_iter_next(num_iter);
        size_iter = g_sequence_iter_next(size_iter);
    }

    g_hash_table_destroy(table);
    g_sequence_free(seq_num);
    g_sequence_free(seq_size);
    return 0;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    unsigned int lb = 1, rb =-1;
	while ((opt = getopt_long(argc, argv, "l:r:", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'l':
                lb = atoi(optarg);
                break;
            case 'r':
                rb = atoi(optarg);
                break;
            default:
                return -1;
        }
    }

    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    get_top_filetypes(20);

    close_database();

    return ret;
}
