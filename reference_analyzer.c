#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

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

    close_iterator("CHUNK");

}

const char * const short_options = "u:";
struct option long_options[] = {
		{ "unit", 0, NULL, 'u' },
		{ NULL, 0, NULL, 0 }
};

int main(int argc, char *argv[])
{
    int opt = 0;
    char *unit = NULL;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {
		switch (opt) {
            case 'u':
                unit = optarg;
                break;
            default:
                return -1;
        }
    }

    open_database("dbhome/");

    if(unit == NULL || strcasecmp(unit, "CHUNK") == 0){
        get_reference_per_chunk();
    }else if(strcasecmp(unit, "REGION") == 0){
        /*get_logical_size_per_region();*/
    }else if(strcasecmp(unit, "CONTAINER") == 0){
        /*get_logical_size_per_container();*/
    }else{
        fprintf(stderr, "invalid unit");
        return -1;
    }

    close_database();

    return 0;
}
