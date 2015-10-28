#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include <inttypes.h>
#include "store.h"

/* output deduplication ratio */
void print_chunk_stat()
{
    init_iterator("CHUNK");

	int64_t logical_size = 0; /* before deduplication */
	int64_t physical_size = 0; /* after deduplication */

	int64_t physical_chunk_num = 0;
	int64_t logical_chunk_num = 0;


    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while (iterate_chunk(&r) == 0) {
		physical_size += r.csize;
		physical_chunk_num++;

		int64_t ls = r.rcount;
		ls *= r.csize;
		logical_size += ls;
		logical_chunk_num += r.rcount;
	}

	fprintf(stderr, "chunk stat:\n");
	fprintf(stderr, "logical size = %"PRId64", physical size = %"PRId64"\n",
			logical_size, physical_size);
	fprintf(stderr, "D/R = %.6f, D/F = %.6f\n", 
			1.0 * (logical_size - physical_size) / logical_size, 
			1.0 * logical_size / physical_size);

	fprintf(stderr, "logical chunk number = %"Prid64", physical chunk number = %"PRId64"\n",
			logical_chunk_num, physical_chunk_num);

    close_iterator("CHUNK");
}

void print_file_stat()
{
    init_iterator("FILE");

	int64_t file_num = 0;
	int64_t logical_size = 0;

	struct file_rec r;
	memset(&r, 0, sizeof(r));
	while (iterate_file(&r) == 0) {
		file_num++;
		logical_size += r.fsize;
	}

	fprintf(stderr, "file number = %"PRId64"\n", file_num);
	fprintf(stderr, "logical size = %"PRId64"\n", logical_size);

    close_iterator("FILE");
}

void get_reference_per_chunk(){
    init_iterator("CHUNK");

	/* for the most popular chunk */
    int max = 1;
    int csize = 0;
    char maxhash[20];

    int count = 0;
    int stat[10];
    memset(stat, 0, sizeof(stat));

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_chunk(&r) == 0){
        fprintf(stdout, "%d\n", r.rcount);
        count++;
        if(r.rcount > max){
            max = r.rcount;
            csize = r.csize;
            memcpy(maxhash, r.hash, 20);
        }
        if(r.rcount > 10){
            stat[9]++;
        }else
            stat[r.rcount-1]++;
    }

    fprintf(stderr, "the most popular chunk:\n");
    fprintf(stderr, "max = %d, size = %d\n", max, csize);
    fprintf(stderr, "hash = %.2hhx:%.2hhx:%.2hhx:%.2hhx:%.2hhx:%.2hhx\n", 
			maxhash[0], maxhash[1], maxhash[2], maxhash[3], maxhash[4], maxhash[5]);

    int i = 0;
    for(;i<10;i++){
        fprintf(stderr, "[%2d : %10.5f]\n", i+1, 1.0*stat[i]/count);
    }

    close_iterator();

}

int main(int argc, char *argv[])
{
    open_database();

	print_chunk_stat();
	print_file_stat();

    close_database();

    return 0;
}
