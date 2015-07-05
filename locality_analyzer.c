#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

int get_distance_per_duplicate_chunk_all(){
    int ret = init_iterator("CHUNK");

    int max = 1;

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r) == 0){
        if(r.rcount == 1)
            continue;

        int prev = r.list[0];
        int i = 1, dist;
        for(; i < r.rcount; i++){
            dist = r.list[i] - prev;
            fprintf(stdout, "%d\n", dist);
            if(max < dist)
                max = dist;
            prev = r.list[i];
        }

    }

    fprintf(stderr, "max = %d\n", max);

    return 0;
}

int get_distance_per_duplicate_chunk_2(){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    while(iterate_chunk(&r) == 0){
        if(r.rcount == 1)
            continue;

        fprintf(stdout, "%d\n", r.list[1] - r.list[0]);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    int all = 0;
	while ((opt = getopt_long(argc, argv, "a", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'a':
                all = 1;
                break;
            default:
                return -1;
        }
    }
    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    if(all == 1)
        get_distance_per_duplicate_chunk_all();
    else
        get_distance_per_duplicate_chunk_2();

    close_database();

    return ret;
}
