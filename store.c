#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <errno.h>
#include <db.h>
#include <glib.h>

#include "data.h"
#include "lru_cache.h"
#include "store.h"

DB_ENV *db_envp = NULL;
DB *chunk_dbp = NULL;
DB *file_dbp = NULL;
DBC *chunk_cursorp = NULL;
DBC *file_cursorp = NULL;

static struct lru_cache *chunk_cache = NULL;
static struct lru_cache *file_cache = NULL;

static void serial_chunk_rec(struct chunk_rec* r, DBT* value)
{

	value->size = sizeof(r->rcount) + sizeof(r->cid) + sizeof(r->rid) +
		sizeof(r->csize) + sizeof(r->cratio) + r->rcount * sizeof(int);

	value->data = malloc(value->size);

	int off = 0;
	memcpy(value->data + off, &r->rcount, sizeof(r->rcount));
	off += sizeof(r->rcount);
	memcpy(value->data + off, &r->cid, sizeof(r->cid));
	off += sizeof(r->cid);
	memcpy(value->data + off, &r->rid, sizeof(r->rid));
	off += sizeof(r->rid);
	memcpy(value->data + off, &r->csize, sizeof(r->csize));
	off += sizeof(r->csize);
	memcpy(value->data + off, &r->cratio, sizeof(r->cratio));
	off += sizeof(r->cratio);

	memcpy(value->data + off, r->list, sizeof(int) * r->rcount);
	off += sizeof(int) * r->rcount;

	assert(off == value->size);
}

static void unserial_chunk_rec(DBT *value, struct chunk_rec *r)
{

	int len = 0;
	memcpy(&r->rcount, value->data, sizeof(r->rcount));
	len += sizeof(r->rcount);
	memcpy(&r->cid, value->data + len, sizeof(r->cid));
	len += sizeof(r->cid);
	memcpy(&r->rid, value->data + len, sizeof(r->rid));
	len += sizeof(r->rid);
	memcpy(&r->csize, value->data + len, sizeof(r->csize));
	len += sizeof(r->csize);
	memcpy(&r->cratio, value->data + len, sizeof(r->cratio));
	len += sizeof(r->cratio);
	if(r->list == NULL) {
		r->list = malloc(sizeof(int) * r->rcount);
		r->listsize = r->rcount;
	} else if (r->listsize > r->rcount * 2 || r->listsize < r->rcount) {
		r->list = realloc(r->list, sizeof(int) * r->rcount);
		r->listsize = r->rcount;
	}

	memcpy(r->list, value->data + len, sizeof(int) * r->rcount);
	len += sizeof(int) * r->rcount;

	assert(len == value->size);
}

static int fingerprint_equal(void *fp1, void *fp2) {
	return !memcmp(fp1, fp2, 20);
}

/* update an existing record to DB */
static void update_chunk_rec(struct chunk_rec *r) {
	DBT key, value;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.data = r->hash;
	key.size = r->hashlen;

	serial_chunk_rec(r, &value);

	int ret = chunk_dbp->put(chunk_dbp, NULL, &key, &value, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	free(value.data);

}

void open_database(char *db_home)
{
	int ret = db_env_create(&db_envp, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	/* DB_CREATE will create db if not existed,
	 * otherwise, be ignored. Right? */
	ret = db_envp->open(db_envp, db_home, DB_CREATE|DB_INIT_MPOOL, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	ret = db_create(&chunk_dbp, db_envp, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}
	ret = db_create(&file_dbp, db_envp, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	ret = chunk_dbp->open(chunk_dbp, NULL, "chunk.db", NULL, DB_HASH, 
			DB_CREATE, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}
	ret = file_dbp->open(file_dbp, NULL, "file.db", NULL, DB_HASH, 
			DB_CREATE, 0);
	if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	chunk_cache = new_lru_cache(1000000, g_int_hash, fingerprint_equal,
			NULL, free_chunk_rec);
	file_cache = new_lru_cache(10000, g_int_hash, g_int_equal,
			NULL, free_file_rec);
}

void close_database()
{
	if (chunk_cache) {
		free_lru_cache(chunk_cache, update_chunk_rec);
	}
	if (file_cache) {
		free_lru_cache(file_cache, NULL);
	}

	chunk_dbp->close(chunk_dbp, 0);
	file_dbp->close(file_dbp, 0);

	db_envp->close(db_envp, 0);
}

/*
 * used in search_chunk and reference_chunk 
 */
static struct chunk_rec *cached_chunk = NULL;

int search_chunk(struct chunk_rec *r)
{
	/*TO-DO: Searching in the cache */
	cached_chunk = lru_cache_lookup(chunk_cache, r->hash);

	if (cached_chunk == NULL) {
		DBT key, value;

		memset(&key, 0, sizeof(DBT));
		memset(&value, 0, sizeof(DBT));

		key.data = r->hash;
		key.size = r->hashlen;

		value.data = NULL;
		value.size = 0;
		value.flags |= DB_DBT_MALLOC;

		int ret = chunk_dbp->get(chunk_dbp, NULL, &key, &value, 0);

		if (ret == DB_NOTFOUND) {
			return STORE_NOTFOUND;
		}

		cached_chunk = malloc(sizeof(*cached_chunk));
		memset(cached_chunk, 0, sizeof(*cached_chunk));
		memcpy(cached_chunk->hash, r->hash, r->hashlen);
		cached_chunk->hashlen = r->hashlen;
		unserial_chunk_rec(&value, cached_chunk);
		lru_cache_insert(chunk_cache, cached_chunk->hash, cached_chunk, 
				update_chunk_rec);

		free(value.data);
	}

	copy_chunk_rec(cached_chunk, r);

	return STORE_EXISTED;
}

/* Already know the key doesn't exist */
void insert_chunk(struct chunk_rec *r)
{
	DBT key, value;

	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.data = r->hash;
	key.size = r->hashlen;

	serial_chunk_rec(r, &value);

	int ret = chunk_dbp->put(chunk_dbp, NULL, &key, &value, DB_NOOVERWRITE);
	if (ret == DB_KEYEXIST) {
		fprintf(stderr, "NOOVERWRITE\n");
		exit(-1);
	} else if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	free(value.data);
}

/* Already know the chunk is in the cache
 * Append logical location and the file ID to list
 * fid is the file ID
 * */
void reference_chunk(struct chunk_rec *r, int fid)
{
	assert(cached_chunk != NULL);

	cached_chunk->rcount++;
	if (cached_chunk->listsize < cached_chunk->rcount) {
		cached_chunk->listsize = cached_chunk->rcount;
		cached_chunk->list = realloc(cached_chunk->list, cached_chunk->listsize);
	}
	cached_chunk->list[cached_chunk->rcount + 1] = fid;
}

static void serial_file_rec(struct file_rec *r, DBT *value)
{
	value->size = sizeof(r->fid) + sizeof(r->cnum) + sizeof(r->fsize) +
		sizeof(r->hash) + sizeof(r->minhash) + sizeof(r->maxhash) + 
		strlen(r->fname);

	assert(value->data == NULL);
	value->data = malloc(value->size);

	int off = 0;
	memcpy(value->data + off, &r->fid, sizeof(r->fid));
	off += sizeof(r->fid);
	memcpy(value->data + off, &r->cnum, sizeof(r->cnum));
	off += sizeof(r->cnum);
	memcpy(value->data + off, &r->fsize, sizeof(r->fsize));
	off += sizeof(r->fsize);
	memcpy(value->data + off, r->hash, sizeof(r->hash));
	off += sizeof(r->hash);
	memcpy(value->data + off, r->minhash, sizeof(r->minhash));
	off += sizeof(r->minhash);
	memcpy(value->data + off, r->maxhash, sizeof(r->maxhash));
	off += sizeof(r->maxhash);
	memcpy(value->data + off, r->fname, strlen(r->fname));
}

static void unserial_file_rec(DBT *value, struct file_rec *r)
{
	int len = 0;
	memcpy(&r->fid, value->data, sizeof(r->fid));
	len += sizeof(r->fid);
	memcpy(&r->cnum, value->data + len, sizeof(r->cnum));
	len += sizeof(r->cnum);
	memcpy(&r->fsize, value->data + len, sizeof(r->fsize));
	len += sizeof(r->fsize);
	memcpy(r->hash, value->data + len, sizeof(r->hash));
	len += sizeof(r->hash);
	memcpy(r->minhash, value->data + len, sizeof(r->minhash));
	len += sizeof(r->minhash);
	memcpy(r->maxhash, value->data + len, sizeof(r->maxhash));
	len += sizeof(r->maxhash);

	if(r->fname == NULL)
		r->fname = malloc(value->size - len + 1);
	else
		r->fname = realloc(r->fname, value->size - len + 1);

	memcpy(r->fname, value->data + len, value->size - len);
	r->fname[value->size - len] = 0;
}

int search_file(struct file_rec *r)
{
	struct file_rec *cached_file = lru_cache_lookup(file_cache, &r->fid);

	if (cached_file == NULL) {
		DBT key, value;
		memset(&key, 0, sizeof(DBT));
		memset(&value, 0, sizeof(DBT));

		key.data = &r->fid;
		key.size = sizeof(r->fid);

		value.data = NULL;
		value.size = 0;
		value.flags |= DB_DBT_MALLOC;

		int ret = file_dbp->get(file_dbp, NULL, &key, &value, 0);
		if (ret == DB_NOTFOUND) {
			return STORE_NOTFOUND;
		}

		cached_file = malloc(sizeof(*cached_file));
		unserial_file_rec(&value, cached_file);
		lru_cache_insert(file_cache, &cached_file->fid, cached_file, NULL);

		free(value.data);
	}

	copy_file_rec(cached_file, r);

	return STORE_EXISTED;
}

void insert_file(struct file_rec *r)
{
	DBT key, value;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	key.data = &r->fid;
	key.size = sizeof(r->fid);

	serial_file_rec(r, &value);

	int ret = file_dbp->put(file_dbp, NULL, &key, &value, DB_NOOVERWRITE);
	if (ret == DB_KEYEXIST) {
		fprintf(stderr, "NOOVERWRITE\n");
		exit(-1);
	} else if (ret != 0) {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	free(value.data);
}

void init_iterator(char *type)
{
	if (strcasecmp(type, "CHUNK") == 0) {
		chunk_dbp->cursor(chunk_dbp, NULL, &chunk_cursorp, 0);
	} else if (strcasecmp(type, "FILE") == 0) {
		file_dbp->cursor(file_dbp, NULL, &file_cursorp, 0);
	} else {
		fprintf(stderr, "invalid iterator!\n");
		exit(-1);
	}
}

void close_iterator(char *type)
{
	if (strcasecmp(type, "CHUNK") == 0) {
		chunk_cursorp->close(chunk_cursorp);
		chunk_cursorp = NULL;
	} else if (strcasecmp(type, "FILE") == 0) {
		file_cursorp->close(file_cursorp);
		file_cursorp = NULL;
	} else {
		fprintf(stderr, "close invalid iterator\n");
		exit(-1);
	}
}

/* return 1: no more data
 * return 0: more data */
/* dedup_fid = 1: remove duplicate file id in file list
 * dedup_fid = 0: do not remove */
int iterate_chunk(struct chunk_rec* r)
{

	DBT key, value;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	int ret = chunk_cursorp->get(chunk_cursorp, &key, &value, DB_NEXT);

	if (ret == DB_NOTFOUND) {
		/* no more data */
		return ITER_STOP;
	} else {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	memcpy(r->hash, key.data, key.size);
	r->hashlen = key.size;

	unserial_chunk_rec(&value, r);

	return ITER_CONTINUE;
}

int iterate_file(struct file_rec* r){

	DBT key, value;
	memset(&key, 0, sizeof(DBT));
	memset(&value, 0, sizeof(DBT));

	int ret = file_cursorp->get(file_cursorp, &key, &value, DB_NEXT);

	if (ret == DB_NOTFOUND) {
		/* no more data */
		return ITER_STOP;
	} else {
		fprintf(stderr, "%s\n", db_strerror(ret));
		exit(-1);
	}

	unserial_file_rec(&value, r);

	return ITER_CONTINUE;
}
