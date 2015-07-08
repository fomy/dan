#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <glib.h>
#include "store.h"

int get_filetypes(){
    int ret = init_iterator("FILE");

    struct file_rec r;
    memset(&r, 0, sizeof(r));

    float sum = 0;
    int count = 0;

    while(iterate_file(&r) == 0)
        fprintf(stdout, "%s\n", r.suffix);

    close_iterator();

    return 0;
}

int main(int argc, char *argv[])
{
    int opt = 0;
    unsigned int lb = 1, rb =-1;
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

    int ret = open_database(argv[optind]);
    if(ret != 0){
        return ret;
    }

    get_filetypes();

    close_database();

    return ret;
}
