#ifndef MEMCACHED_CACHE_H
#define MEMCACHED_CACHE_H

typedef struct {
    pthread_mutex_t mutex;
    char *name;
    void **ptr;
    size_t bufsize;
    int freetotal;
    int freecurr;
    // cache_constructor_t *constructor;
    // cache_constructor_t *destructor;
} cache_t;

cache_t *cache_create(const char *, size_t, size_t, void *, void *);

#endif
