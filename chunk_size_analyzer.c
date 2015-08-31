#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include "store.h"

int get_chunksize_distribution(unsigned int lb, unsigned int rb){
    init_iterator("CHUNK");

    int64_t sum = 0;
    int count = 0;

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r, 0) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            fprintf(stdout, "%d\n", r.csize);
            sum += r.csize;
            count++;
        }
    }

    fprintf(stderr, "avg. chunk size = %10.2f\n", 1.0*sum/count);

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

    open_database();

    get_chunksize_distribution(lb, rb);

    close_database();

    return 0;
}
