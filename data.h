#ifndef DATA_H_
#define DATA_H_

#define CONTAINER_SIZE 4194304
#define COMPRESSION_REGION_SIZE 131072

/* chunk record */
struct chunk_rec{
    /* fingerprint */
    char hash[20];
    int hashlen;
    /* reference number */
    int rcount;
    /* container id (4MB container as in DDFS)*/
    int cid;
    /* compression region id (128KB before compression as in DDFS)*/
    /* almost 2X compression ratio as reported in [Wallace2012] */
    int rid;
    /* chunk size */
    int csize;
    /* compression ratio for each chunk */
    float cratio;
    /* location list and its buffer size
     * rcount is the actual number of items in llist */
    int lsize;
    int *llist;
};

/* containr record */
struct container_rec{
    int cid;
    /* logical size */
    int lsize;
    /* physical size */
    int psize;
};

/* compression region record */
struct region_rec{
    int rid;
    /* logical size */
    int lsize;
    /* physical size */
    int psize;
};

struct file_rec{
    int fid;
    int fsize;
    /* chunk number */
    int cnum;
};

void init_chunk_rec(struct chunk_rec *r);
void reset_chunk_rec(struct chunk_rec *r);

void init_container_rec(struct container_rec* r);
int container_full(struct container_rec* r, int csize);

void init_region_rec(struct region_rec* r);
int region_full(struct region_rec* r, int csize);

#endif
