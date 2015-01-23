#ifndef MEMCACHED_HASH_H
#define MEMCACHED_HASH_H

typedef uint32_t (*hash_func)(const void *, size_t);
hash_func hash;

enum hashfunc_type {
    JENKINS_HASH = 0,
    MURMUR3_HASH
};

int hash_init(enum hashfunc_type);

#endif
