#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

void get_file2ref_ratio(unsigned int lb, unsigned int rb){
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

}

void analyze_references(unsigned int lb, unsigned int rb){
    int ret = init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int total_references = 0;
    int intra_file = 0;
    int identical_file = 0;
    int min_similar_file = 0;
    int similar_file = 0;
    int distinct_file = 0;
    while(iterate_chunk(&r) == 0){

        if(r.rcount >= lb && r.rcount <= rb){
            total_references += r.rcount - 1;
            intra_file += r.rcount - r.fcount;
            struct file_rec files[r.fcount];
            int i = 0;
            for(; i < r.fcount; i++){
                files[i].fid = r.list[r.lsize/2 + i];
                search_file(&files[i]);
            }
            
            /* analyze files */
            int identical = 0, min_similar = 0, similar = 0;
            for(i=1; i<r.fcount; i++){
                int j = 0;
                for(; j<i; j++){
                    if(memcmp(files[i].hash, files[j].hash, sizeof(files[i].hash)) == 0){
                        identical = 1;
                        break;
                    }else if(memcmp(files[i].maxhash, files[j].maxhash, sizeof(files[i].maxhash)) == 0){
                        min_similar = 1;
                    }else if(memcmp(files[i].minhash, files[j].minhash, sizeof(files[i].minhash)) == 0){
                        similar = 1;
                    }
                }
                if(identical == 1)
                    identical_file++;
                else if(min_similar == 1)
                    min_similar_file++;
                else if(similar == 1)
                    similar_file++;
                else
                    distinct_file++;
            }
        }
    }

    close_iterator();
    assert(total_references == intra_file + identical_file + min_similar_file + similar_file + distinct_file);

    /*fprintf(stderr, "%8s %8s %8s %8s %8s\n", "Total", "Intra", "Ident", "Simi", "Dist");*/
    /*fprintf(stdout, "%8d %8d %8d %8d %8d\n", total_references, intra_file, identical_file, similar_file, distinct_file);*/
    fprintf(stderr, "%8s %8s %8s %8s %8s\n", "Intra", "Ident", "Min", "+Max", "Dist");
    fprintf(stdout, "%8.5f %8.5f %8.5f %8.5f %8.5f\n", 1.0*intra_file/total_references, 1.0*identical_file/total_references, 
            1.0*min_similar_file/total_references, 1.0*similar_file/total_references, 1.0*distinct_file/total_references);
}


int main(int argc, char *argv[])
{
    int opt = 0;
    unsigned int lb = 2, rb =-1;
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

    /* makes no sense to analyze reference number 1 */
    assert(lb > 1);
    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    /*get_file2ref_ratio(lb, rb);*/
    analyze_references(lb, rb);

    close_database();

    return ret;
}
