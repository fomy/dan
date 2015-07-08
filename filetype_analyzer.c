#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include "store.h"

struct suffix{
    /* The number of files with this suffix */
    int num;
    /* The size of files with this suffix */
    int64_t size;
    char suffix[8];
};

static gboolean suffix_equal(gconstpointer a, gconstpointer b){
    return strcmp(a, b) == 0;
}

int get_filetypes(){
    int ret = init_iterator("FILE");

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    /* File system */
    int fs_count = 0;
    int fs_size = 0;

    GHashTable *table = g_hash_table_new_full(g_int_hash, suffix_equal, NULL, free);
    struct suffix *s = NULL;
    while(iterate_file(&r) == 0){
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

    fprintf(stderr, "totally %d suffix in %d files\n", g_hash_table_size(table), fs_count);


    g_hash_table_destroy(table);
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

    get_filetypes();

    close_database();

    return ret;
}
