#ifndef DATA_H_
#define DATA_H_

/* 4MB-sized container */
#define CONTAINER_SIZE 4194304
/* 128KB-sized compression region.
 * Each container has 32 regions. */
#define COMPRESSION_REGION_SIZE 131072
#define REGIONS_PER_CONTAINER 32

/* chunk record */
struct chunk_rec {
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
	/* number of items is defined by rcount, equivalent to rcount */
    int *list;
	int listsize;
};

/* containr record */
struct container_rec {
    int cid;
    /* logical size */
    int lsize;
    /* physical size */
    int psize;
    /* the empty slots for compression region */
    int slots;
};

/* compression region record */
struct region_rec {
    int rid;
    /* logical size */
    int lsize;
    /* physical size */
    int psize;
};

struct file_rec {
    int fid;
    /* chunk number */
    int cnum;
    int64_t fsize;
    /* The file hash */
    char hash[20];
    /* The minimal hash */
    char minhash[20];
    /* The maximal hash */
    char maxhash[20];
    /* The file name */
    char *fname;
};

void reset_chunk_rec(struct chunk_rec *r);
void free_chunk_rec(struct chunk_rec *r);
void copy_chunk_rec(struct chunk_rec *r, struct chunk_rec *copy);
void reset_container_rec(struct container_rec *r);
int container_full(struct container_rec* r);
void reset_region_rec(struct region_rec* r);
int add_chunk_to_region(struct chunk_rec* c, struct region_rec* r);
int add_region_to_container(struct region_rec* r, struct container_rec* c);
void free_file_rec(struct file_rec *r);
void copy_file_rec(struct file_rec *r, struct file_rec *c);

void parse_file_suffix(char *path, char *suffix, int suffixlen);

#endif
