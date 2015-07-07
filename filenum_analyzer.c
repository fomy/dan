#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include "store.h"

int get_file_num_per_duplicate_chunk(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    float sum = 0;
    int count = 0;

    while(iterate_chunk(&r) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            fprintf(stdout, "%10.2f\n", 1.0*r.fcount/r.rcount);
            sum += 1.0*r.fcount/r.rcount;
            count++;
        }
    }

    fprintf(stderr, "avg: %10.2f\n", sum/count);

    close_iterator();

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

    get_file_num_per_duplicate_chunk(lb, rb);

    close_database();

    return ret;
}
