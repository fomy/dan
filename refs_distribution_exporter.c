#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

int get_reference_per_chunk(){
    int ret = init_iterator("CHUNK");

    int max = 1;
    int count = 0;
    int stat[10];
    memset(stat, 0, sizeof(stat));

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_chunk(&r) == 0){
        fprintf(stdout, "%d\n", r.rcount);
        count++;
        if(r.rcount > max)
            max = r.rcount;
        if(r.rcount > 10){
            stat[9]++;
        }else
            stat[r.rcount-1]++;
    }

    fprintf(stderr, "max = %d\n", max);
    int i = 0;
    for(;i<10;i++){
        fprintf(stderr, "[%2d : %10.5f]\n", i+1, 1.0*stat[i]/count);
    }

    close_iterator();

    return 0;
}

int get_logical_size_per_container(){
    int ret = init_iterator("CONTAINER");

    float max = 1;
    int count = 0;

    int stat[10];
    memset(stat, 0, sizeof(stat));

    struct container_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_container(&r) == 0){
        count++;
        float factor = 1.0 * r.lsize / CONTAINER_SIZE;
        fprintf(stdout, "%10.2f\n", factor < 1.0 ? 1.0 : factor);
        if(factor > max)
            max = factor;
        if(factor >= 10){
            stat[9]++;
        }else{
            int level = factor;
            /*printf("%d, %f\n", level, factor);*/
            stat[level]++;
        }
    }

    fprintf(stderr, "max = %10.2f\n", max);
    int i = 0;
    for(;i<10;i++){
        fprintf(stderr, "[%2d : %10.5f]\n", i+1, 1.0*stat[i]/count);
    }

    return 0;
}

int get_logical_size_per_region(){
    int ret = init_iterator("REGION");

    float max = 1;
    int count = 0;

    int stat[10];
    memset(stat, 0, sizeof(stat));

    struct region_rec r;
    memset(&r, 0, sizeof(r));
    while(iterate_region(&r) == 0){
        count++;
        float factor = 1.0 * r.lsize / COMPRESSION_REGION_SIZE;
        fprintf(stdout, "%10.2f\n", factor < 1.0 ? 1.0 : factor);
        if(factor > max)
            max = factor;
        if(factor >= 10){
            stat[9]++;
        }else{
            int level = factor;
            /*printf("%d, %f\n", level, factor);*/
            stat[level]++;
        }
    }

    fprintf(stderr, "max = %10.2f\n", max);
    int i = 0;
    for(;i<10;i++){
        fprintf(stderr, "[%2d : %10.5f]\n", i+1, 1.0*stat[i]/count);
    }

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

    if(unit == NULL || strcasecmp(unit, "CHUNK") == 0){
        ret = get_reference_per_chunk();
    }else if(strcasecmp(unit, "REGION") == 0){
        ret = get_logical_size_per_region();
    }else if(strcasecmp(unit, "CONTAINER") == 0){
        ret = get_logical_size_per_container();
    }else{
        fprintf(stderr, "invalid unit");
        return -1;
    }

    close_database();

    return ret;
}