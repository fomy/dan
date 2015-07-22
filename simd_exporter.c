#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>
#include "store.h"

void output_simd_trace(){
    init_iterator("CHUNK");

    struct chunk_rec r;
    memset(&r, 0, sizeof(r));

    int count = 0;
    while(iterate_chunk(&r, 1) == 0){

        printf("%d ", r.rcount);
        struct file_rec file;
        memset(&file, 0, sizeof(file));
        int i = 0;
        int64_t size = 0;
        for(; i < r.fcount; i++){
            file.fid = r.list[r.rcount + i];
            search_file(&file);
            size += file.fsize;
        }
        printf("%"PRId64"\n", size);
    }

    close_iterator();

}


int main(int argc, char *argv[])
{
    open_database();

    output_simd_trace();

    close_database();

    return 0;
}
