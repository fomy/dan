#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

int distance_between_all_chunk_references(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    int64_t sum = 0;
    int count = 0;

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r, 0) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            int prev = r.list[0];
            int i = 1, dist;
            for(; i < r.rcount; i++){
                dist = r.list[i] - prev;
                assert(dist > 0);
                fprintf(stdout, "%d\n", dist);
                prev = r.list[i];

                sum += dist;
                count++;
            }
        }
    }

    fprintf(stderr, "avg. = %10.2f\n", 1.0*sum/count);

    close_iterator();

    return 0;
}

int distance_between_first_two_chunk_references(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t sum = 0;
    int count = 0;

    while(iterate_chunk(&r, 0) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            /*fprintf(stderr, "%d\n", r.rcount);*/
            assert(r.list[1] > r.list[0]);
            fprintf(stdout, "%d\n", r.list[1] - r.list[0]);
            sum += r.list[1] - r.list[0];
            count++;
        }
    }

    fprintf(stderr, "avg. = %10.2f\n", 1.0*sum/count);
    close_iterator();
    return 0;
}

/*Level: Low (2), Mid (3-4), High (5-infi)*/

int main(int argc, char *argv[])
{
    int opt = 0;
    int all = 0;
    unsigned int lb = 2, rb =2;
	while ((opt = getopt_long(argc, argv, "al:r:", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'a':
                /* all distances */
                all = 1;
                break;
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

    assert(lb >= 2);
    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    if(all == 1)
        distance_between_all_chunk_references(lb, rb);
    else
        distance_between_first_two_chunk_references(lb, rb);

    close_database();

    return ret;
}
