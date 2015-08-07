#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>
#include <glib.h>
#include "libhashfile.h"
#include "store.h"

#define MODEA 1
#define MODEB 2
#define MODEC 3

static gboolean hash20_equal(gpointer a, gpointer b){
    return !memcmp(a, b, 20);
}

/* In mode A, only LSE model 
 * Only Dedup trace
 */
void modeA_simd_trace(char *path){
    printf("MODE A:DEDUP\n");
    init_iterator("CHUNK");

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));

    int64_t psize = 0;
    int64_t lsize = 0;
    while(iterate_chunk(&chunk, 0) == 0){

        int64_t sum = chunk.csize;
        sum *= chunk.rcount;

        lsize += sum;
        psize += chunk.csize;
        printf("%"PRId64"\n", sum);
    }

    printf("%.6f\n", 1.0*lsize/psize);
    fprintf(stderr, "D/F = %.4f\n", 1.0*lsize/psize);

    close_iterator();

    char buf[4096];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;

    handle = hashfile_open(path);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(-1);
    }

    int64_t restore_logical_bytes = 0;
    int64_t restore_physical_bytes = 0;
    GHashTable* chunks = g_hash_table_new_full(g_int_hash, hash20_equal, free, NULL);

    /* 1 - 99 */
    int step = 1;
    /* All chunks lost */
    puts("0");

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

            if(!g_hash_table_contains(chunks, chunk.hash)){
                search_chunk(&chunk);
                int64_t sum = chunk.csize;
                sum *= chunk.rcount;

                restore_physical_bytes += chunk.csize;
                restore_logical_bytes += sum;

                int progress = restore_physical_bytes * 100/psize;
                while(progress >= step && step <= 99){
                    printf("%.6f\n", 1.0*restore_logical_bytes/lsize);
                    step++;
                }
                char* hash = malloc(20);
                memcpy(hash, chunk.hash, 20);
                g_hash_table_insert(chunks, hash, hash);

            }

        }
    }
    g_hash_table_destroy(chunks);
    hashfile_close(handle);

    puts("1.0");
}

void modeBC_nodedup_simd_trace(char *path, int mode){
    if(mode == MODEB)
        printf("MODE B:NO DEDUP\n");
    else
        printf("MODE C:NO DEDUP\n");

    int64_t sys_capacity = 0;
    int64_t sys_file_number = 0;

    init_iterator("CHUNK");

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));
    struct file_rec fr;
    memset(&fr, 0, sizeof(fr));

    while(iterate_chunk(&chunk, 0) == 0){

        int64_t sum = chunk.csize;
        sum *= chunk.rcount;
        sys_capacity += sum;
        int i = 0;
        int prev = -1;
        for(; i<chunk.rcount; i++){
            int fid = chunk.list[chunk.rcount+i];
            fr.fid = fid;
            search_file(&fr);

            prev = fid;
            if(mode == MODEC)
                printf("%"PRId64"\n", fr.fsize);
        }
    }

    close_iterator();

    sys_file_number = get_file_number();

    fprintf(stderr, "capacity = %.4f GB, Files = %"PRId64"\n", 1.0*sys_capacity/1024/1024/1024, sys_file_number);

    char buf[4096];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;

    handle = hashfile_open(path);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(-1);
    }

    /* All files lost */
    puts("0");

    int64_t restore_bytes = 0;
    int64_t restore_files = 0;
    int64_t restore_file_bytes = 0;

    /* 1 - 99 */
    int step = 1;
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
            /* It will overflow */
            /*restore_bytes += ci->size;*/
            int size = ci->size;
            restore_bytes += size;
            filesize += size;

            int progress = restore_bytes * 100 / sys_capacity;
            while(progress >= step && step <= 99){
                if(mode == MODEB)
                    printf("%.6f\n", 1.0*restore_files/sys_file_number);
                else
                    printf("%.6f\n", 1.0*restore_file_bytes/sys_capacity);
                step++;
            }
        }

        assert(filesize == hashfile_curfile_size(handle));
        if(hashfile_curfile_size(handle) == 0)
            continue;

        restore_files++;
        restore_file_bytes += hashfile_curfile_size(handle);

    }

    hashfile_close(handle);
    puts("1.0");
}

struct restoring_file{
    int id;
    int chunk_num;
    int64_t size;
};

void modeBC_dedup_simd_trace(char* path, int mode){
    if(mode == MODEB)
        printf("MODE B:DEDUP\n");
    else
        printf("MODE C:DEDUP\n");

    init_iterator("CHUNK");

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));
    struct file_rec fr;
    memset(&fr, 0, sizeof(fr));

    int64_t psize = 0;
    int64_t lsize = 0;
    while(iterate_chunk(&chunk, 0) == 0){

        int64_t sum = chunk.csize;
        sum *= chunk.rcount;
        lsize += sum;
        psize += chunk.csize;
        if(mode == MODEB){
            printf("%d\n", chunk.fcount);
        }else{
            int i = 0;
            int prev = -1;
            int64_t sum = 0;
            for(; i<chunk.rcount; i++){
                int fid = chunk.list[chunk.rcount+i];
                if(fid == prev)
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
    fprintf(stderr, "LS = %.4f GB, PS = %.4f GB, D/F = %.4f\n", 1.0*lsize/1024/1024/1024,
            1.0*psize/1024/1024/1024, 1.0*lsize/psize);

    close_iterator();

    char buf[4096];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;

    int64_t sys_file_number = get_file_number();

    handle = hashfile_open(path);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(-1);
    }

    /* All files lost */
    puts("0");

    int64_t restore_bytes = 0;
    int64_t restore_files = 0;
    int64_t restore_file_bytes = 0;

    /* 1 - 99 */
    int step = 1;

    GHashTable* files = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, free);
    GHashTable* chunks = g_hash_table_new_full(g_int_hash, hash20_equal, free, NULL);

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

            if(!g_hash_table_contains(chunks, chunk.hash)){
                /* restore a chunk */
                search_chunk(&chunk);
                int i = 0;
                for(;i<chunk.rcount; i++){
                    int fid = chunk.list[chunk.rcount + i];
                    struct restoring_file* rfile = g_hash_table_lookup(files, &fid);
                    if(!rfile){
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
                while(progress >= step && step <= 99){
                    if(mode == MODEB)
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

    g_hash_table_destroy(files);
    g_hash_table_destroy(chunks);
    fprintf(stderr, "restore %.4f GB\n", 1.0*restore_file_bytes/1024/1024/1024);

    hashfile_close(handle);
    puts("1.0");
}

int main(int argc, char *argv[])
{
    int dedup = 1;
    int check_file_size = 0;
    int opt = 0;
    int mode = MODEA;
    char *path = NULL;
    while ((opt = getopt_long(argc, argv, "m:nf:", NULL, NULL))
            != -1) {
        switch (opt) {
            case'm':
                mode = atoi(optarg);
                break;
            case 'n':
                /* disable deduplication */
                dedup = 0;
                break;
            case 'f':
                path = optarg;
                break;
            default:
                return -1;
        }
    }

    open_database();
    if(mode == MODEA){
        modeA_simd_trace(path);
    }else{
        assert(mode == MODEB || mode == MODEC);
        if(!dedup)
            modeBC_nodedup_simd_trace(path, mode);
        else{
            modeBC_dedup_simd_trace(path, mode);
        }
    }
    close_database();

    return 0;
}
