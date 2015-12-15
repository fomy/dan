#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include <inttypes.h>
#include "store.h"

static void print_hash(const uint8_t *hash,
        int hash_size_in_bytes)
{
    int j;
    printf("%.2hhx", hash[0]);
    for (j = 1; j < hash_size_in_bytes; j++)
        printf(":%.2hhx", hash[j]);
    printf("\n");
}

void get_reference_per_chunk()
{
    init_iterator("CHUNK");

    int max = 1;
    int csize = 0;
    char maxhash[20];

    int count = 0;
	/* stat[10] for >10 */
	/* stat[11] for >20 */
    int stat[12];
    memset(stat, 0, sizeof(stat));

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while (iterate_chunk(&r, 0) == 0) {
        fprintf(stdout, "%d\n", r.rcount);
        count++;
        if(r.rcount > max){
            max = r.rcount;
            csize = r.csize;
            memcpy(maxhash, r.hash, 20);
        }
        if (r.rcount > 10) {
            stat[10]++;
			if (r.rcount > 20)
				stat[11]++;
        } else
            stat[r.rcount - 1]++;
    }

    fprintf(stderr, "max = %d, size = %d\n", max, csize);
    fprintf(stderr, "hash = %.2hhx:%.2hhx:%.2hhx:%.2hhx:%.2hhx:%.2hhx\n", 
			maxhash[0], maxhash[1], maxhash[2], maxhash[3], maxhash[4], maxhash[5]);
    int i = 0;
    for (; i < 10; i++) {
        fprintf(stderr, "[%2d : %10.5f]\n", i+1, 1.0*stat[i]/count);
    }
    fprintf(stderr, "[>10 : %10.5f]\n", 1.0*stat[10]/count);
    fprintf(stderr, "[>20 : %10.5f]\n", 1.0*stat[11]/count);

    close_iterator();

}

void output_popular_chunks(int h)
{
    init_iterator("CHUNK");

	int count = 0;
	int pop_count = 0;

	int64_t logical_size = 0;
	int64_t physical_size = 0;

	int64_t pop_physical_size = 0;
	int64_t pop_logical_size = 0;
	
    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while (iterate_chunk(&r, 0) == 0) {
        count++;

		physical_size += r.csize;

		int64_t ls = r.csize;
		ls *= r.rcount;
		logical_size += ls;

        if (r.rcount > h) {
			pop_count++;

			pop_physical_size += r.csize;

			pop_logical_size += ls;

			print_hash(r.hash, 20);
        }
    }

	fprintf(stderr, "percentage in count: %.6f\n",
			1.0*pop_count/count);
	fprintf(stderr, "percentage in physical capacity: %.6f\n", 
			1.0*pop_physical_size/physical_size);
	fprintf(stderr, "percentage in logical capacity: %.6f\n",
			1.0*pop_logical_size/logical_size);
    close_iterator();

}

const char * const short_options = "p:";
struct option long_options[] = {
		{ "popular", 0, NULL, 'p' },
		{ NULL, 0, NULL, 0 }
};

int main(int argc, char *argv[])
{
    int opt = 0;
	int popular_chunks = 0;
	int h = 10;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {
		switch (opt) {
            case 'p':
				popular_chunks = 1;
				h = atoi(optarg);
                break;
            default:
                return -1;
        }
    }

    open_database();

    if (popular_chunks == 0) {
        get_reference_per_chunk();
    } else {
		output_popular_chunks(h);
    }

    close_database();

    return 0;
}
