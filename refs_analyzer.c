#include <stdlib.h>
#include <stdio.h>
#include <string.h>
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

    fprintf(stdout, "max = %d\n", max);
    int i = 0;
    for(;i<128;i++){
        fprintf(stdout, "[%d : %d]\n", i+1, stat[i]);
    }

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

    fprintf(stdout, "max = %10.2f\n", max);

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

    fprintf(stdout, "max = %10.2f\n", max);

    return 0;
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		fprintf(stderr, "Wrong usage!\n");
		fprintf(stderr, "Usage: %s <hashfile>\n", argv[0]);
		return -1;
	}

    int ret = open_database(argv[1]);
    if(ret != 0){
        return ret;
    }

	/*ret = get_reference_per_chunk();*/
    ret = get_logical_size_per_container();
    /*ret = get_logical_size_per_region();*/

    close_database();

    return ret;
}
