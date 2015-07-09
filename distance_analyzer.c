#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

int get_distance_per_duplicate_chunk_all(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    int max = 1;

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            int prev = r.list[0];
            int i = 1, dist;
            for(; i < r.rcount; i++){
                dist = r.list[i] - prev;
                assert(dist > 0);
                fprintf(stdout, "%d\n", dist);
                if(max < dist)
                    max = dist;
                prev = r.list[i];
            }
        }
    }

    fprintf(stderr, "max = %d\n", max);

    close_iterator();

    return 0;
}

int get_distance_per_duplicate_chunk_first(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r) == 0){
        if(r.rcount >= lb && r.rcount <= rb){
            /*fprintf(stderr, "%d\n", r.rcount);*/
            assert(r.list[1] > r.list[0]);
            fprintf(stdout, "%d\n", r.list[1] - r.list[0]);
        }
    }

    return 0;
}

/*Level: Low (2), Mid (3-4), High (5-infi)*/

int main(int argc, char *argv[])
{
    int opt = 0;
    int all = 0;
    unsigned int lb = 2, rb =2;
	while ((opt = getopt_long(argc, argv, "al:", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'a':
                /* all distances */
                all = 1;
                break;
            case 'l':
                if(strcasecmp(optarg, "Low") == 0){
                    lb = 2;
                    rb = 2;
                }else if(strcasecmp(optarg, "Mid") == 0){
                    lb = 3;
                    rb = 4;
                }else if(strcasecmp(optarg, "High") == 0){
                    lb = 5;
                    rb = -1;
                }else if(strcasecmp(optarg, "All") == 0){
                    lb = 2;
                    rb = -1;
                }else{
                    fprintf(stderr, "Invalid level\n");
                    return -1;
                }
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
        get_distance_per_duplicate_chunk_all(lb, rb);
    else
        get_distance_per_duplicate_chunk_first(lb, rb);

    close_database();

    return ret;
}
