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
        }
        stat[r.rcount-1]++;
    }

    fprintf(stdout, "max = %d\n", max);
    int i = 0;
    for(;i<128;i++){
        fprintf(stdout, "[%d : %d]\n", i+1, stat[i]);
    }

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

	ret = get_reference_per_chunk();

    close_database();

    return ret;
}
