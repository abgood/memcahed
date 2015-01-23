#include "memcached.h"

cache_t *cache_create(const char *name, size_t bufsize, size_t align, void *a, void *b) {
    printf("cache_create\n");
    return (cache_t *)1;
}
