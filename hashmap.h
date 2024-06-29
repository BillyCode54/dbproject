#include <stddef.h>
#ifndef HASHMAP_H
#define HASHMAP_H

typedef struct HashNode {
    char* key;
    int value;
    struct HashNode* next;
} HashNode;

typedef struct {
    size_t size;
    HashNode** buckets;
} Hashmap;

Hashmap* hashmap_create(size_t initial_size);
void hashmap_destroy(Hashmap* map);
int hashmap_get(Hashmap* map, const char* key);
void hashmap_put(Hashmap* map, const char* key, int value);

#endif /* HASHMAP_H */
