/*
 * cache.c
 *
 *  Created on: May 23, 2012
 *      Author: fumin
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "lru_cache.h"

static void free_lru_elem(struct lru_cache *cache, struct lru_elem *elem) {
	if (cache->key_free) 
		cache->key_free(elem->key);
	if (cache->value_free)
		cache->value_free(elem->data);

	elem->next = NULL;
	elem->prev = NULL;
	free(elem);
}

/*
 * The container read cache.
 */
struct lru_cache* new_lru_cache(int cache_size, 
		int (*key_hash)(void *),
		int (*key_equal)(void *, void *),
		void (*key_free)(void *),
		void (*value_free)(void *))
{
	struct lru_cache *c = (struct lru_cache*) malloc(sizeof(*c));

	c->max_size = cache_size;
	c->size = 0;
	c->hit_count = 0;
	c->miss_count = 0;

	c->key_free = key_free;
	c->value_free = value_free;

	c->key_hash = key_hash;
	c->key_equal = key_equal;

	/*assert(c->key_free != NULL);*/
	assert(c->value_free != NULL);
	assert(c->key_hash != NULL);
	assert(c->key_equal != NULL);

	c->head = NULL;
	c->tail = NULL;
	c->index = g_hash_table_new(c->key_hash, c->key_equal);

	return c;
}

void free_lru_cache(struct lru_cache* c, 
		void (*victim_handler)(void *))
{
	/* output */
	fprintf(stderr, "hit=%d (total=%d), hit ratio=%.4f\n", c->hit_count,
			c->miss_count + c->hit_count, 
			1.0*c->hit_count/(c->hit_count+c->miss_count));

	struct lru_elem *next;
	while (c->head) {
		next = c->head->next;

		/* before free them, flush them to disks */
		if (victim_handler)
			victim_handler(c->head->data);
		free_lru_elem(c, c->head);

		c->head = next;
	}

	g_hash_table_destroy(c->index);
	free(c);
}

/* look for user data */
void* lru_cache_lookup(struct lru_cache *c, void *key)
{

	struct lru_elem *elem = g_hash_table_lookup(c->index, key);

	if (elem == NULL) {
		c->miss_count++;
		return NULL;
	}

	if (elem->prev != NULL) { /* it is not head */
		elem->prev->next = elem->next;

		assert(c->head != elem);

		if (elem->next == NULL) /* it is tail */
			c->tail = elem->prev;
		else
			elem->next->prev = elem->prev;

		elem->next = c->head;
		c->head->prev = elem;
		elem->prev = NULL;
		c->head = elem;
	}

	c->hit_count++;
	return elem->data;
}

/*
 * We know that the data does not exist!
 * victim_handler is used to handle victim
 */
void lru_cache_insert(struct lru_cache *c, void *key, void *value,
		void (*victim_handler)(void*))
{
	struct lru_elem *elem = NULL;
	if (lru_cache_is_full(c)) {
		elem = c->tail;

		assert(elem->prev != NULL);
		c->tail = elem->prev;
		elem->prev->next = NULL;

		if (victim_handler)
			victim_handler(elem->data);

		g_hash_table_remove(c->index, elem->key);

		if (c->key_free) c->key_free(elem->key);
		if (c->value_free) c->value_free(elem->data);

		c->size--;
	} else {
		elem = malloc(sizeof(*elem));
	}

	memset(elem, 0, sizeof(*elem));

	elem->key = key;
	elem->data = value;

	elem->next = c->head;
	elem->prev = NULL;
	if (c->head)
		c->head->prev = elem;
	else
		c->tail = elem;
	c->head = elem;

	g_hash_table_insert(c->index, key, elem);

	c->size++;
}

int lru_cache_is_full(struct lru_cache* c)
{
	if (c->max_size < 0)
		return 0;
	return c->size >= c->max_size ? 1 : 0;
}
