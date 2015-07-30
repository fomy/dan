#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include <getopt.h>
#include "store.h"
void output_nodedup_simd_trace(){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t physical_size = 0;
    int64_t logical_size = 0;

    double sum = 0;
    int64_t count = 0;
    while(iterate_chunk(&r, 0) == 0){

        physical_size += r.csize;
        int64_t rc = r.rcount;
        logical_size += r.csize * rc;

        struct file_rec file;
        memset(&file, 0, sizeof(file));
        int i = 0;
        int64_t size = 0;
        for(; i < r.rcount; i++){
            file.fid = r.list[r.rcount + i];
            search_file(&file);
            size += file.fsize;
            count++;
            sum += 1.0*file.fsize/1024/1024;
            printf("%"PRId64"\n", file.fsize);
        }
    }

    /* The last number if the deduplication ratio */
    printf("%.1f\n", 1.0);

    fprintf(stderr, "Physical Size = %"PRId64", Logical Size = %"PRId64", D/R = %.4f\n", physical_size, logical_size, 1.0*logical_size/physical_size);
    fprintf(stderr, "Sum = %.4f MB, Count = %"PRId64", Avg. = %.4f MB\n", sum, count, sum/count);

    close_iterator();

}

void output_dedup_simd_trace(){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t physical_size = 0;
    int64_t logical_size = 0;

    double sum = 0;
    int64_t count = 0;
    while(iterate_chunk(&r, 1) == 0){

        physical_size += r.csize;
        int64_t rc = r.rcount;
        logical_size += r.csize * rc;

        struct file_rec file;
        memset(&file, 0, sizeof(file));
        int i = 0;
        int64_t size = 0;
        for(; i < r.fcount; i++){
            file.fid = r.list[r.rcount + i];
            search_file(&file);
            size += file.fsize;
        }
        count++;
        sum += 1.0*size/1024/1024;
        printf("%"PRId64"\n", size);
    }

    /* The last number if the deduplication ratio */
    printf("%.4f\n", 1.0*logical_size/physical_size);

    fprintf(stderr, "Physical Size = %"PRId64", Logical Size = %"PRId64", D/R = %.4f\n", physical_size, logical_size, 1.0*logical_size/physical_size);
    fprintf(stderr, "Sum = %.4f MB, Count = %"PRId64", Avg. = %.4f MB\n", sum, count, sum/count);

    close_iterator();

}


int main(int argc, char *argv[])
{
    int dedup = 1;
    int opt = 0;
    while ((opt = getopt_long(argc, argv, "n", NULL, NULL))
            != -1) {
        switch (opt) {
            case 'n':
                /* disable deduplication */
                dedup = 0;
                break;
            default:
                return -1;
        }
    }

    open_database();

    if(dedup)
        output_dedup_simd_trace();
    else
        output_nodedup_simd_trace();

    close_database();

    return 0;
}
