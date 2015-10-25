#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>
#include <glib.h>
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include "libhashfile.h"
#include "store.h"

#define MODEA 1
#define MODEB 2
#define MODEC 3

static gboolean hash20_equal(gpointer a, gpointer b){
    return !memcmp(a, b, 20);
}

/* The number of lines relies on chunksize;
 * Only chunk-level trace requires this  */
static void print_a_chunk(int chunksize, int64_t content){
    int lines_no = (chunksize+1023)/1024;
    assert(lines_no >= 1);
    assert(lines_no <= 16);
    int i = 0;
    for(; i<lines_no; i++)
        printf("%"PRId64"\n", content);
}

void reverse_trace(char *path, char* reverse_file){
    char buf[4096];
    struct hashfile_handle *handle;
    const struct chunk_info *ci;

    handle = hashfile_open(path);

    if (!handle) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(-1);
    }

    GHashTable* hashes = g_hash_table_new_full(g_int_hash, hash20_equal, NULL, NULL);
    GSequence *hashqueue = g_sequence_new(free);

    char hash[20];
    memset(hash, 0, 20);
    int hashlen = 0;
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

        fprintf(stderr, "%s, %"PRIu64"\n", hashfile_curfile_path(handle), hashfile_curfile_size(handle));
        while (1) {
            ci = hashfile_next_chunk(handle);
            if (!ci) /* exit the loop if it was the last chunk */
                break;
            int hashsize = hashfile_hash_size(handle)/8;
            int chunksize = ci->size;
            memcpy(hash, ci->hash, hashsize);
            memcpy(&hash[hashsize], &chunksize, sizeof(chunksize));
            hashlen = hashfile_hash_size(handle)/8 + sizeof(chunksize);

            if(!g_hash_table_contains(hashes, hash)){
                char* newhash = malloc(20);
                memcpy(newhash, hash, 20);
                g_sequence_prepend(hashqueue, newhash);
                g_hash_table_insert(hashes, newhash, NULL);
            }
        }
    }
    hashfile_close(handle);

    g_hash_table_destroy(hashes);

    fprintf(stderr, "queue size = %d\n", g_sequence_get_length(hashqueue));

    int fd = open(reverse_file, O_CREAT|O_WRONLY);
    if(fd < 0){
        fprintf(stderr, "cannot open file %s\n", reverse_file);
        exit(-1);
    }

    fprintf(stderr, "hashlen = %d\n", hashlen);
    write(fd, &hashlen, sizeof(hashlen));
    char* nexthash = NULL;

    GSequenceIter *iter = g_sequence_get_begin_iter(hashqueue);
    while(!g_sequence_iter_is_end(iter)){
        write(fd, g_sequence_get(iter), hashlen);
        iter = g_sequence_iter_next(iter);
    }

    close(fd);
    g_sequence_free(hashqueue);
}

/*  
 * Chunk level, without file semantics 
 * Dedup
 * (no trace for chunk-level no-dedup model)
 */
void chunk_dedup_simd_trace(char *path, int weighted){
    if(weighted){
        fprintf(stderr, "CHUNK:DEDUP:WEIGHTED\n");
        printf("CHUNK:DEDUP:WEIGHTED\n");
    }else{
        fprintf(stderr, "CHUNK:DEDUP:NOT WEIGHTED\n");
        printf("CHUNK:DEDUP:NOT WEIGHTED\n");
    }

    init_iterator("CHUNK");

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));

    int64_t psize = 0;
    int64_t lsize = 0;
    int64_t total_chunks = 0;
    /* USE part */
    while(iterate_chunk(&chunk, 0) == 0){

        int64_t sum = chunk.csize;
        sum *= chunk.rcount;

        lsize += sum;
        psize += chunk.csize;

        total_chunks += chunk.rcount;

        if(weighted){
            print_a_chunk(chunk.csize, sum);
        }else{
            print_a_chunk(chunk.csize, chunk.rcount);
        }
    }

    printf("%.6f\n", 1.0*lsize/psize);
    fprintf(stderr, "D/F = %.4f, total_chunks = %"PRId64"\n", 1.0*lsize/psize, total_chunks);

    close_iterator();

    int fd = open(path, O_RDONLY);

    if (fd < 0) {
        fprintf(stderr, "Error opening hash file: %d!", errno);
        exit(-1);
    }

    int64_t restore_logical_bytes = 0;
    int64_t restore_physical_bytes = 0;
    int64_t restore_chunks = 0;

    /* RAID Failure part */
    /* 1 - 99 */
    int step = 1;
    /* All chunks lost */
    puts("0");

    int byte = read(fd, &chunk.hashlen, 4);
    assert(byte == 4);

    byte = read(fd, chunk.hash, chunk.hashlen);
    while (byte > 0) {

        assert(search_chunk(&chunk));
        int64_t sum = chunk.csize;
        sum *= chunk.rcount;
        restore_chunks += chunk.rcount;

        restore_physical_bytes += chunk.csize;
        restore_logical_bytes += sum;

        int progress = restore_physical_bytes * 100/psize;
        while(progress >= step && step <= 99){
            if(weighted){
                printf("%.6f\n", 1.0*restore_logical_bytes/lsize);
                fprintf(stderr, "%.6f\n", 1.0*restore_logical_bytes/lsize);
            }else{
                printf("%.6f\n", 1.0*restore_chunks/total_chunks);
                fprintf(stderr, "%.6f\n", 1.0*restore_chunks/total_chunks);
            }
            step++;
        }
        byte = read(fd, chunk.hash, chunk.hashlen);
        assert(restore_logical_bytes <= lsize);
    }

    close(fd);
    puts("1.0");
}

struct restoring_file{
    int id;
    int chunk_num;
    int64_t size;
};

void file_dedup_simd_trace(char* path, int weighted){
    if(weighted){
        printf("FILE:DEDUP:WEIGHTED\n");
        fprintf(stderr, "FILE:DEDUP:WEIGHTED\n");
    }else{
        printf("FILE:DEDUP:NOT WEIGHTED\n");
        fprintf(stderr, "FILE:DEDUP:NOT WEIGHTED\n");
    }

    init_iterator("CHUNK");

    struct chunk_rec chunk;
    memset(&chunk, 0, sizeof(chunk));
    struct file_rec fr;
    memset(&fr, 0, sizeof(fr));

    /* USE part */
    int64_t lsize = 0;
    int64_t psize = 0;
    while(iterate_chunk(&chunk, 0) == 0){

        int64_t sum = chunk.csize;
        sum *= chunk.rcount;
        lsize += sum;
        psize += chunk.csize;
        if(!weighted){
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

    int64_t sys_file_number = get_file_number();

    int fd = open(path, O_RDONLY);

    /* All files lost */
    puts("0");

    int64_t restore_bytes = 0;
    int64_t restore_files = 0;
    int64_t restore_file_bytes = 0;

    /* RAID Failure part */
    /* 1 - 99 */
    int step = 1;

    GHashTable* files = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, free);

    int byte = read(fd, &chunk.hashlen, 4);
    assert(byte == 4);

    while(1){
        byte = read(fd, chunk.hash, chunk.hashlen);
        if(byte != chunk.hashlen)
            break;

        /* restore a chunk */
        assert(search_chunk(&chunk));
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
            if(!weighted){
                printf("%.6f\n", 1.0*restore_files/sys_file_number);
                fprintf(stderr, "%.6f\n", 1.0*restore_files/sys_file_number);
            }else{
                printf("%.6f\n", 1.0*restore_file_bytes/lsize);
                fprintf(stderr, "%.6f\n", 1.0*restore_file_bytes/lsize);
            }
            step++;
        }
        assert(restore_file_bytes <= lsize);
    }

    g_hash_table_destroy(files);
    fprintf(stderr, "restore %.4f GB\n", 1.0*restore_file_bytes/1024/1024/1024);

    close(fd);
    puts("1.0");
}

int main(int argc, char *argv[])
{
    /* dedup? */
    int dedup = 0;
    /* weighted by file/chunk size? */
    int weighted = 0;
    /* chunk level or file level */
    int file_level = 0;

    int reverse = 0;
    char* reverse_file = NULL;

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "ro:fwd", NULL, NULL))
            != -1) {
        switch (opt) {
            case'f':
                file_level = 1;
                break;
            case 'd':
                dedup = 1;
                break;
            case 'w':
                weighted = 1;
                break;
            case 'r':
                reverse = 1;
                break;
            case 'o':
                reverse_file = optarg;
                break;
            default:
                fprintf(stderr, "invalid option\n");
                return -1;
        }
    }

    char *path = argv[optind];

    if(reverse){
        reverse_trace(path, reverse_file);
        return 0;
    }

    open_database();

    if(!file_level){
        chunk_dedup_simd_trace(path, weighted);
    }else{
        file_dedup_simd_trace(path, weighted);
    }
    close_database();

    return 0;
}