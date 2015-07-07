#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

int get_reference_per_chunk(){
    int ret = init_iterator("CHUNK");

    int max = 1;
    int stat[128];
    memset(stat, 0, sizeof(stat));

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_chunk(&r) == 0){
        fprintf(stdout, "%d\n", r.rcount);
        if(r.rcount > max)
            max = r.rcount;
        if(r.rcount > 128){
            /*printf("highly referenced chunk, %d\n", r.rcount);*/
            stat[127]++;
        }else
            stat[r.rcount-1]++;
    }

    fprintf(stderr, "max = %d\n", max);
    int i = 0;
    for(;i<128;i++){
        fprintf(stderr, "[%d : %d]\n", i+1, stat[i]);
    }

    close_iterator();

    return 0;
}

int get_logical_size_per_container(){
    int ret = init_iterator("CONTAINER");

    float max = 1;

    struct container_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_container(&r) == 0){
        float factor = 1.0 * r.lsize / CONTAINER_SIZE;
        fprintf(stdout, "%10.2f\n", factor < 1.0 ? 1.0 : factor);
        if(factor > max)
            max = factor;
    }

    fprintf(stderr, "max = %10.2f\n", max);

    return 0;
}

int get_logical_size_per_region(){
    int ret = init_iterator("REGION");

    float max = 1;

    struct region_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_region(&r) == 0){
        float factor = 1.0 * r.lsize / COMPRESSION_REGION_SIZE;
        fprintf(stdout, "%10.2f\n", factor < 1.0 ? 1.0 : factor);
        if(factor > max)
            max = factor;
    }

    fprintf(stderr, "max = %10.2f\n", max);

    return 0;
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

    assert(optind < argc);
    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    if(unit == NULL || strcmp(unit, "CHUNK") == 0 || strcmp(unit, "chunk") == 0){
        ret = get_reference_per_chunk();
    }else if(strcmp(unit, "REGION") == 0 || strcmp(unit, "region") == 0){
        ret = get_logical_size_per_region();
    }else if(strcmp(unit, "CONTAINER") == 0 || strcmp(unit, "container") == 0){
        ret = get_logical_size_per_container();
    }else{
        fprintf(stderr, "invalid unit");
        return -1;
    }

    close_database();

    return ret;
}
