#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include <inttypes.h>
#include "store.h"

struct suffix_stat{
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

static gint suffix_num_cmp(struct suffix_stat* a, struct suffix_stat* b, gpointer data){
    return b->num > a->num ? 1:-1;
}

static gint suffix_size_cmp(struct suffix_stat* a, struct suffix_stat* b, gpointer data){
    return b->size > a->size ? 1:-1;
}

void get_selected_filetypes(GHashTable* typeset, int64_t *fs_count, int64_t* fs_size, unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    GHashTable *files = g_hash_table_new_full(g_int_hash, g_int_equal, free, NULL);

    /*fprintf(stderr, "Find the files\n");*/
    while(iterate_chunk(&r, 1) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            /* Get all files' IDs */
            int i = 0;
            for(; i<r.fcount; i++){
                int fid = r.list[r.lsize/2 + i];
                if(!g_hash_table_contains(files, &fid)){
                    int *new = malloc(sizeof(int));
                    *new = fid;
                    g_hash_table_insert(files, new, NULL);
                }
            }
        }
    }

    /* Now, all required files are in files table */
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, files);

    struct file_rec file;
    struct suffix_stat *s = NULL;
    char suffix[8];
    while(g_hash_table_iter_next(&iter, &key, &value)){
        file.fid = *(int*)key;
        if(search_file(&file) != 1){
            fprintf(stderr, "Cannot find the required file %d in file db\n", file.fid);
            exit(-1);
        }

        parse_file_suffix(file.fname, suffix, sizeof(suffix));

        if(strncmp(suffix, "edu,", 4) == 0){
            strcpy(suffix, "edu,?");
        }else if(strlen(suffix) == 0){
            strcpy(suffix, ".None");
        }
        if((s = g_hash_table_lookup(typeset, suffix)) == NULL){
            s = malloc(sizeof(struct suffix_stat));
            memcpy(s->suffix, suffix, sizeof(suffix));
            s->num = 0;
            s->size = 0;
            g_hash_table_insert(typeset, s->suffix, s);
        }
        s->num++;
        s->size += file.fsize;

        *fs_size += file.fsize;

    }
    *fs_count = g_hash_table_size(files);

    g_hash_table_destroy(files);

    fprintf(stderr, "totally %d suffix in %" PRId64 " files\n", g_hash_table_size(typeset), *fs_count);
}

void get_all_filetypes(GHashTable *typeset, int64_t *fs_count, int64_t *fs_size){
    int ret = init_iterator("FILE");

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    struct suffix_stat *s = NULL;
    char suffix[8];
    while(iterate_file(&r) == 0){
        parse_file_suffix(r.fname, suffix, sizeof(suffix));
        if(strncmp(suffix, "edu,", 4) == 0){
            strcpy(suffix, "edu,?");
        }else if(strlen(suffix) == 0){
            strcpy(suffix, ".None");
        }
        if((s = g_hash_table_lookup(typeset, suffix)) == NULL){
            s = malloc(sizeof(struct suffix_stat));
            memcpy(s->suffix, suffix, sizeof(suffix));
            s->num = 0;
            s->size = 0;
            g_hash_table_insert(typeset, s->suffix, s);
        }
        s->num++;
        s->size += r.fsize;

        (*fs_count)++;
        *fs_size += r.fsize;
    }
    close_iterator();

    fprintf(stderr, "totally %d suffix in %" PRId64 " files\n", g_hash_table_size(typeset), *fs_count);
}

void get_top_filetypes(GHashTable *typeset, int64_t fs_count, int64_t fs_size, int top){

    GSequence* seq_num = g_sequence_new(NULL), *seq_size = g_sequence_new(NULL);

    GHashTableIter iter;
    g_hash_table_iter_init(&iter, typeset);
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

    printf("%10s %10s %10s %10s\n", "#type", "num", "type", "size");
    int i = 0;
    int64_t total_num = 0, total_size = 0;
    GSequenceIter* num_iter = g_sequence_get_begin_iter(seq_num);
    GSequenceIter* size_iter = g_sequence_get_begin_iter(seq_size);
    for(; i<top; i++){
        struct suffix_stat* num_suffix = g_sequence_get(num_iter);
        struct suffix_stat* size_suffix = g_sequence_get(size_iter);
        total_num += num_suffix->num;
        total_size += size_suffix->size;
        printf("%10s %10.4f %10s %10.4f\n", num_suffix->suffix, 1.0*num_suffix->num/fs_count, size_suffix->suffix, 1.0*size_suffix->size/fs_size);
        num_iter = g_sequence_iter_next(num_iter);
        size_iter = g_sequence_iter_next(size_iter);
    }
    printf("%10s %10.4f %10s %10.4f\n", "Sum", 1.0*total_num/fs_count, "Sum", 1.0*total_size/fs_size);
    printf("%10s %" PRId64 " %10s %" PRId64 "\n", "#Total", fs_count, "Total(MB)", fs_size/1024/1024);

    g_sequence_free(seq_num);
    g_sequence_free(seq_size);
}

int main(int argc, char *argv[])
{
    int opt = 0;
    unsigned int lb = 1, rb =-1;
    int top = 10;
	while ((opt = getopt_long(argc, argv, "l:r:t:", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'l':
                lb = atoi(optarg);
                break;
            case 'r':
                rb = atoi(optarg);
                break;
            case 't':
                top = atoi(optarg);
                break;
            default:
                return -1;
        }
    }

    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    /* File system */
    int64_t fs_count = 0;
    int64_t fs_size = 0;

    GHashTable *types= g_hash_table_new_full(g_int_hash, suffix_equal, NULL, free);

    if(lb == 1 && rb == -1)
        get_all_filetypes(types, &fs_count, &fs_size);
    else
        get_selected_filetypes(types, &fs_count, &fs_size, lb, rb);

    get_top_filetypes(types, fs_count, fs_size, top);

    g_hash_table_destroy(types);

    close_database();

    return ret;
}
