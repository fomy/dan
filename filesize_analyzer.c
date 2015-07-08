#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include <inttypes.h>
#include "store.h"

int get_filesize_distribution(){
    int ret = init_iterator("FILE");

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    int64_t sum = 0;
    int count = 0;

    while(iterate_file(&r) == 0){
        fprintf(stdout, "%" PRId64 "\n", r.fsize);
        sum += r.fsize;
        count++;
    }

    fprintf(stderr, "avg: %10.2f\n", 1.0*sum/count);

    close_iterator();

    return 0;
}

int get_filesize_distribution_by_refs(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t sum = 0;
    /* file count */
    int file_count = 0;
    int chunk_count = 0;

    GHashTable *files = g_hash_table_new_full(g_int_hash, g_int_equal, free, NULL);

    fprintf(stderr, "Find the files\n");
    while(iterate_chunk(&r) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            /* Get all files' IDs */
            chunk_count++;
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

    close_iterator();

    /* Now, all required files are in files table */
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, files);

    struct file_rec file;
    while(g_hash_table_iter_next(&iter, &key, &value)){
        file.fid = *(int*)key;
        if(search_file(&file) != 1){
            fprintf(stderr, "Cannot find the required file %d in file db\n", file.fid);
            exit(-1);
        }
        fprintf(stdout, "%" PRId64 "\n", file.fsize);
        sum += file.fsize;
        file_count++;
    }

    g_hash_table_destroy(files);

    fprintf(stderr, "avg: %10.2f\n", 1.0*sum/file_count);
    fprintf(stderr, "chunk count: %d\n", chunk_count);

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

    if(lb == 1 && rb == -1)
        get_filesize_distribution();
    else
        get_filesize_distribution_by_refs(lb, rb);

    close_database();

    return ret;
}
