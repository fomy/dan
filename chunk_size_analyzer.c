#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

void avg_chunksize(){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t sum_nodedup = 0;
    int64_t count_nodedup = 0;
    int64_t sum_removed = 0;
    int64_t count_removed = 0;
    int64_t sum_stored = 0;
    int64_t count_stored = 0;

    while(iterate_chunk(&r, 0) == 0){
        int64_t tmp = r.csize;
        tmp *= r.rcount;
        sum_nodedup += tmp;
        count_nodedup += r.rcount;

        tmp = r.csize;
        tmp *= r.rcount - 1;
        sum_removed += tmp;
        count_removed += r.rcount - 1;

        sum_stored += r.csize;
        count_stored++;
    }

    close_iterator();

    fprintf(stderr, "nodedup = %10.2f\n", 1.0*sum_nodedup/count_nodedup);
    fprintf(stderr, "removed = %10.2f\n", 1.0*sum_removed/count_removed);
    fprintf(stderr, "stored = %10.2f\n", 1.0*sum_stored/count_stored);
}

int get_chunksize_distribution(unsigned int lb, unsigned int rb){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int64_t sum_a = 0;
    int64_t sum_u = 0;
    int64_t sum_d = 0;
    int64_t sum_l = 0;
    int64_t sum_m = 0;
    int64_t sum_h = 0;
    int64_t sum_eh = 0;

    int count_a = 0;
    int count_u = 0;
    int count_d = 0;
    int count_l = 0;
    int count_m = 0;
    int count_h = 0;
    int count_eh = 0;

    while(iterate_chunk(&r, 0) == 0){

        sum_a += r.csize;
        count_a++;

        if(r.rcount == 1){
            sum_u += r.csize;
            count_u++;
        }else{
            assert(r.rcount >= 2);
            sum_d += r.csize;
            count_d++;

            if(r.rcount <= 2){
                sum_l += r.csize;
                count_l++;
            }else if(r.rcount <= 4){
                sum_m += r.csize;
                count_m++;
            }else if(r.rcount <=7){
                sum_h += r.csize;
                count_h++;
            }else{
                sum_eh += r.csize;
                count_eh++;
            }
        }

        if(r.rcount >= lb && r.rcount <= rb){
            fprintf(stdout, "%d\n", r.csize);
        }
    }

    fprintf(stderr, "ALL = %10.2f\n", 1.0*sum_a/count_a);
    fprintf(stderr, "UNI = %10.2f\n", 1.0*sum_u/count_u);
    fprintf(stderr, "DUP = %10.2f\n", 1.0*sum_d/count_d);
    fprintf(stderr, "LOW = %10.2f\n", 1.0*sum_l/count_l);
    fprintf(stderr, "MID = %10.2f\n", 1.0*sum_m/count_m);
    fprintf(stderr, "HIGH = %10.2f\n", 1.0*sum_h/count_h);
    fprintf(stderr, "EH = %10.2f\n", 1.0*sum_eh/count_eh);

    close_iterator();

    return 0;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    unsigned int lb = 1, rb =-1;
    int distribution = 0;
	while ((opt = getopt_long(argc, argv, "l:r:d", NULL, NULL))
			!= -1) {
		switch (opt) {
            case 'l':
                lb = atoi(optarg);
                break;
            case 'r':
                rb = atoi(optarg);
                break;
            case 'd':
                distribution = 1;
                break;
            default:
                return -1;
        }
    }

    open_database();

    if(distribution)
        get_chunksize_distribution(lb, rb);
    else
        avg_chunksize();

    close_database();

    return 0;
}
