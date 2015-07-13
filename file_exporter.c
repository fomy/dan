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

/* sharing some chunks but with different hash/minhash */
void collect_distinct_files(){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int count = 0;
    while(iterate_chunk(&r) == 0){

        if(r.rcount > 1 && r.fcount > 1){
            struct file_rec files[r.fcount];
            memset(files, 0, sizeof(files));
            int i = 0;
            for(; i < r.fcount; i++){
                files[i].fid = r.list[r.lsize/2 + i];
                search_file(&files[i]);
            }
            for(i=1; i<r.fcount; i++){
                if(memcmp(files[i].minhash, files[i-1].minhash, sizeof(files[i].minhash)) != 0){
                    char suffix[8];
                    printf("CHUK %d ", r.fcount);
                    print_hash(r.hash, 6);
                    int j = 0;
                    for(; j<r.fcount; j++){
                        parse_file_suffix(files[j].fname, suffix, sizeof(suffix));
                        if(strncmp(suffix, "edu,", 4) == 0){
                            strcpy(suffix, "edu,?");
                        }else if(strlen(suffix) == 0){
                            strcpy(suffix, ".None");
                        }
                        printf("FILE %d %" PRId64 " %s %s ", files[j].fid, files[j].fsize,
                                files[j].fname, suffix);
                        print_hash(files[j].minhash, 6);
                    }

                    count++;
                    break;
                }
            }
        }
    }
    fprintf(stderr, "%d chunks are shared between distinct files\n", count);

    close_iterator();

}

static int check_identical_files(struct file_list* fl){
    GList* cur = fl->head;
    GList* next = NULL;
    while((next = g_list_next(cur))){
        if(!file_equal(cur->data, next->data))
            return 0;
        cur = next;
    }
    return 1;
}

int collect_similar_files(){
    int ret = init_iterator("FILE");
    if(ret != 0)
        return ret;

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    GHashTable* hashset = g_hash_table_new_full(g_int_hash, hash_equal, NULL, free_file_list);

    int empty_files = 0;
    while(iterate_file(&r) == 0){
        if(r.fsize > 0){
            struct file_list* fl = g_hash_table_lookup(hashset, r.minhash);
            if(fl == NULL){
                fl = malloc(sizeof(struct file_list));
                fl->head = NULL;
                memcpy(fl->hash, r.minhash, sizeof(r.minhash));
                g_hash_table_insert(hashset, fl->hash, fl);
            }

            struct file_item* item = malloc(sizeof(*item) + strlen(r.fname) + 1);
            item->fid = r.fid;
            item->fsize = r.fsize;
            memcpy(item->hash, r.hash, sizeof(r.hash));
            strcpy(item->fname, r.fname);
            fl->head = g_list_prepend(fl->head, item);
        }else
            empty_files++;
    }

    close_iterator();

    fprintf(stderr, "%d bins, %d empty files\n", g_hash_table_size(hashset), empty_files);
    g_hash_table_foreach_remove(hashset, only_one_item, NULL);

    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, hashset);
    char suffix[8];
    int iden_count = 0;
    while(g_hash_table_iter_next(&iter, &key, &value)){
        struct file_list* fl = value;
        /* remove identical files */
        if(check_identical_files(fl)){
            iden_count++;
            continue;
        }

        printf("HASH %d ", g_list_length(fl->head));
        print_hash(fl->hash, 6);
        GList* elem = g_list_first(fl->head);
        do{
            struct file_item* item = elem->data;
            parse_file_suffix(item->fname, suffix, sizeof(suffix));
            if(strncmp(suffix, "edu,", 4) == 0){
                strcpy(suffix, "edu,?");
            }else if(strlen(suffix) == 0){
                strcpy(suffix, ".None");
            }
            printf("FILE %d %" PRId64 " %s %s ", item->fid, item->fsize,
                    item->fname, suffix);
            print_hash(item->hash, 6);
        }while((elem = g_list_next(elem)));
    }

    fprintf(stderr, "%d bins, from %d of which all files are identical\n", g_hash_table_size(hashset), iden_count);
    g_hash_table_destroy(hashset);

    return 0;
}

int collect_identical_files(){
    int ret = init_iterator("FILE");
    if(ret != 0)
        return ret;

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    GHashTable* hashset = g_hash_table_new_full(g_int_hash, hash_equal, NULL, free_file_list);

    while(iterate_file(&r) == 0){
        if(r.fsize > 0){
            struct file_list* fl = g_hash_table_lookup(hashset, r.hash);
            if(fl == NULL){
                fl = malloc(sizeof(struct file_list));
                fl->head = NULL;
                memcpy(fl->hash, r.hash, sizeof(r.hash));
                g_hash_table_insert(hashset, fl->hash, fl);
            }

            struct file_item* item = malloc(sizeof(*item) + strlen(r.fname) + 1);
            item->fid = r.fid;
            item->fsize = r.fsize;
            strcpy(item->fname, r.fname);
            fl->head = g_list_prepend(fl->head, item);
        }
    }

    close_iterator();

    g_hash_table_foreach_remove(hashset, only_one_item, NULL);

    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, hashset);
    char suffix[8];
    while(g_hash_table_iter_next(&iter, &key, &value)){
        struct file_list* fl = value;
        printf("HASH %d ", g_list_length(fl->head));
        print_hash(fl->hash, 6);
        GList* elem = g_list_first(fl->head);
        do{
            struct file_item* item = elem->data;
            parse_file_suffix(item->fname, suffix, sizeof(suffix));
            if(strncmp(suffix, "edu,", 4) == 0){
                strcpy(suffix, "edu,?");
            }else if(strlen(suffix) == 0){
                strcpy(suffix, ".None");
            }
            printf("FILE %d %" PRId64 " %s %s\n", item->fid, item->fsize,
                    item->fname, suffix);
        }while((elem = g_list_next(elem)));
    }

    g_hash_table_destroy(hashset);
    return 0;
}

#define FLAG_IDENTICAL_FILES 1
#define FLAG_SIMILAR_FILES 2
#define FLAG_DISTINCT_FILES 3

int main(int argc, char *argv[])
{
    int opt = 0;
    int flag = FLAG_IDENTICAL_FILES;
    while ((opt = getopt_long(argc, argv, "isd", NULL, NULL))
            != -1) {
        switch (opt) {
            case 'i':
                flag = FLAG_IDENTICAL_FILES;
                break;
            case 's':
                flag = FLAG_SIMILAR_FILES;
                break;
            case 'd':
                flag = FLAG_DISTINCT_FILES;
                break;
            default:
                return -1;
        }
    }

    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    if(flag == FLAG_IDENTICAL_FILES)
        collect_identical_files();
    else if(flag == FLAG_SIMILAR_FILES)
        collect_similar_files();
    else if(flag == FLAG_DISTINCT_FILES)
        collect_distinct_files();

    close_database();

    return ret;
}
