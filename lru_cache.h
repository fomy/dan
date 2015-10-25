/*
 * lru_cache.h
 *	GList-based lru cache
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#ifndef CACHE_H_
#define CACHE_H_

#include <glib.h>

struct lru_elem {
	void *key;
	void *data;
	struct lru_elem *next;
	struct lru_elem *prev;
};

/* Just a hash table linked by an LRU queue */
struct lru_cache {
	GHashTable *index;
	struct lru_elem *head;
	struct lru_elem *tail;

	int max_size; // less then zero means infinite cache
	int size; // current number of elems

	int hit_count;
	int miss_count;

	void (*key_free)(void *);
	void (*value_free)(void *);

	int (*key_hash)(void *);
	int (*key_equal)(void *, void *);
};

struct lru_cache* new_lru_cache(int cache_size, 
		int (*key_hash)(void *),
		int (*key_equal)(void *, void *),
		void (*key_free)(void *),
		void (*value_free)(void *)
		);
void free_lru_cache(struct lru_cache* c, 
		void (*victim_handler)(void *));
/* get the data identified by the key */
void* lru_cache_lookup(struct lru_cache*, void*);
/* insert the data and its key */
/* victim handler should flush value to DB, but not free them */
/* let the hash table free them */
void lru_cache_insert(struct lru_cache*, void*, void*,
		void (*victim_handler)(void*));
int lru_cache_is_full(struct lru_cache*);

#endif /* CACHE_H_ */
