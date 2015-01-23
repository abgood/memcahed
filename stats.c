#include "memcached.h"

#define PREFIX_HASH_SIZE 256

typedef struct _prefix_stats PREFIX_STATS;
struct _prefix_stats {
    char *prefix;
    size_t prefix_len;
    uint64_t num_gets;
    uint64_t num_sets;
    uint64_t num_deletes;
    uint64_t num_hits;
    PREFIX_STATS *next;
};

static PREFIX_STATS *prefix_stats[PREFIX_HASH_SIZE];

void stats_prefix_init(void) {
    memset(prefix_stats, 0, sizeof(prefix_stats));
}
