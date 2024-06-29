#include "hashmap.h"
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdio.h>

#define INITIAL_CAPACITY 100

unsigned long hash(const char* str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

Hashmap* hashmap_create(size_t initial_size) {
    Hashmap* map = (Hashmap*)malloc(sizeof(Hashmap));
    if (map == NULL) {
        return NULL;
    }

    map->size = initial_size;
    map->buckets = (HashNode**)calloc(initial_size, sizeof(HashNode*));
    if (map->buckets == NULL) {
        free(map);
        return NULL;
    }

    return map;
}

void hashmap_destroy(Hashmap* map) {
    if (map == NULL) {
        return;
    }

    for (size_t i = 0; i < map->size; ++i) {
        HashNode* node = map->buckets[i];
        while (node != NULL) {
            HashNode* temp = node;
            node = node->next;
            free(temp->key);
            free(temp);
        }
    }

    free(map->buckets);
    free(map);
}

int hashmap_get(Hashmap* map, const char* key) {
    unsigned long index = hash(key) % map->size;
    HashNode* node = map->buckets[index];

    while (node != NULL) {
        if (strcmp(node->key, key) == 0) {
            return node->value;
        }
        node = node->next;
    }

    return -1;
}

void hashmap_put(Hashmap* map, const char* key, int value) {
    unsigned long index = hash(key) % map->size;
    HashNode* node = map->buckets[index];

    while (node != NULL) {
        if (strcmp(node->key, key) == 0) {
            node->value = value;
            return;
        }
        node = node->next;
    }

    HashNode* new_node = (HashNode*)malloc(sizeof(HashNode));
    if (new_node == NULL) {
        printf("allocatin failed for %s\n",key);
        return;
    }

    new_node->key = strdup(key);
    new_node->value = value;
    new_node->next = map->buckets[index];
    map->buckets[index] = new_node;
}
