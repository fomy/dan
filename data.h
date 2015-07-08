#ifndef DATA_H_
#define DATA_H_

/* 4MB-sized container */
#define CONTAINER_SIZE 4194304
/* 128KB-sized compression region.
 * Each container has 32 regions. */
#define COMPRESSION_REGION_SIZE 131072
#define REGIONS_PER_CONTAINER 32

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
    /* The number of files referencing the chunk,
     * which is smaller than rcount */
    int fcount;
    /* The list stores the locations and the files referring to it
     * lsize is the list size 
     * The first part (1/2) includes locations, and the second part (1/2) includes file IDs.
     * The actual number of locations is rcount;
     * the actual number of file IDs is fcount  */
    int lsize;
    int *list;
};

/* containr record */
struct container_rec{
    int cid;
    /* logical size */
    int lsize;
    /* physical size */
    int psize;
    /* the empty slots for compression region */
    int slots;
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
    /* chunk number */
    int cnum;
    int64_t fsize;
    /* The file suffix */
    char suffix[8];
    /* The file hash */
    char hash[20];
    /* The minimal hash */
    char minhash[20];
};

void reset_chunk_rec(struct chunk_rec *r);
void reset_container_rec(struct container_rec *r);
int container_full(struct container_rec* r);
void reset_region_rec(struct region_rec* r);
int add_chunk_to_region(struct chunk_rec* c, struct region_rec* r);
int add_region_to_container(struct region_rec* r, struct container_rec* c);
int check_file_list(int *list, int fcount, int fid);

#endif
