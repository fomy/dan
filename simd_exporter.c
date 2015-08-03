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

/* In mode A, only LSE model 
 * Only Dedup trace
 */
void modeA_simd_trace(){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t psize = 0;
    int64_t lsize = 0;
    while(iterate_chunk(&r, 0) == 0){

        lsize += r.csize * r.rcount;
        psize += r.csize;
        printf("%d\n", r.csize * r.rcount);
    }

    printf("%.4f\n", 1.0*lsize/psize);
    fprintf(stderr, "D/F = %.4f\n", 1.0*lsize/psize);

    close_iterator();

}

void modeB_nodedup_simd_trace(char *path){
    char buf[4096];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;

    int64_t sys_capacity = 0;
    int64_t sys_file_number = 0;

    handle = hashfile_open(path);

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

            sys_capacity += ci->size;
        }

        sys_file_number++;
    }

    hashfile_close(handle);

    fprintf(stderr, "capacity = %.4f GB, Files = %"PRId64"\n", 1.0*sys_capacity/1024/1024/1024, sys_file_number);

    handle = hashfile_open(path);

    /* All files lost */
    puts("1");

    int64_t restore_bytes = 0;
    int64_t restore_files = 0;

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

        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;
            restore_bytes += ci->size;
        }

        restore_files++;

        int progress = restore_bytes * 100 / sys_capacity;
        while(progress >= step && step <= 99){
            printf("%.4f\n", 1-1.0*restore_files/sys_file_number);
            step++;
        }
    }

    hashfile_close(handle);
}

void modeB_dedup_simd_trace(char* path){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t psize = 0;
    int64_t lsize = 0;
    while(iterate_chunk(&r, 0) == 0){

        lsize += r.csize * r.rcount;
        psize += r.csize;
        printf("%d\n", r.fcount);
    }

    printf("%.4f\n", 1.0*lsize/psize);
    fprintf(stderr, "LS = %.4 GB, PS = %.4 GB, D/F = %.4f\n", 1.0*lsize/1024/1024/1024,
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

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));
    int chunk_count = 0;

    /* All files lost */
    puts("1");

    int restore_bytes = 0;
    int restore_files = 0;

    /* 1 - 99 */
    int step = 1;

    GHashTable* files = g_hash_table_new_full(g_int_hash, g_int_equal, free, free);

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

            if(search_chunk(&chunk) != 1){
                fprintf(stderr, "Cannot find the chunk\n");
                exit(-1);
            }

            if(chunk.list[0] != chunk_count){
                /* skip this chunk */
                assert(chunk.list[0] < chunk_count);
            }else{
                /* restore a chunk */
                int i = 0;
                for(;i<chunk.rcount; i++){
                    int fid = chunk.list[chunk.rcount + i];
                    int* chunknum = g_hash_table_lookup(files, &fid);
                    if(!chunknum){
                        struct file_rec fr;
                        memset(&fr, 0, sizeof(fr));
                        fr.fid = fid;
                        search_file(&fr);

                        chunknum = malloc(sizeof(int));
                        *chunknum = fr.cnum;
                        int* file_id = malloc(sizeof(int));
                        *file_id = fid;

                        g_hash_table_insert(files, file_id, chunknum);
                    }
                    *chunknum--;

                    if(*chunknum == 0){
                        /* a file is restored */
                        fprintf(stderr, "complete file %d\n", fid);
                        restore_files++;
                    }
                    assert(*chunknum >= 0);
                }

                restore_bytes += chunk.csize;
                int progress = restore_bytes * 100/psize;
                while(progress >= step && step <= 99){
                    printf("%.4f\n", 1-1.0*restore_files/sys_file_number);
                    step++;
                }
            }

            chunk_count++;
        }
    }

    hashfile_close(handle);
}

#define MODEA 1
#define MODEB 2
#define MODEC 3

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

    if(mode == MODEA){
        modeA_simd_trace();
    }else if(mode == MODEB){
        if(!dedup)
            modeB_nodedup_simd_trace(path);
        else{
            open_database();

            modeB_dedup_simd_trace(path);

            close_database();
        }
    }

    return 0;
}
