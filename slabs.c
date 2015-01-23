#include "memcached.h"

typedef struct {
    unsigned int size;
    unsigned int perslab;
    unsigned int sl_curr;
    unsigned int slabs;
    unsigned int list_size;
    unsigned int killing;

    void *slots;
    void **slab_list;
    size_t requested;
} slabclass_t;

static size_t mem_limit = 0;
static size_t mem_avail = 0;
static void *mem_base = NULL;
static void *mem_current = NULL;
static int power_largest;

static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];

static pthread_cond_t slab_rebalance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t maintenance_tid;
static pthread_t rebalance_tid;

#define DEFAULT_SLAB_BULK_CHECK 1
int slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

void slabs_init(const size_t limit, const double factor, const bool prealloc) {
    int i = POWER_SMALLEST - 1;
    // sizeof(item) = 48
    unsigned int size = sizeof(item) + settings.chunk_size;

    mem_limit = limit;

    if (prealloc) {
        mem_base = malloc(mem_limit);
        if (mem_base) {
            mem_current = mem_base;
            mem_avail = mem_limit;
        } else {
            fprintf(stderr, "Warning: Failed to allocate requested memory in "
                    "one large chunk.\nWill allocate in smaller chunks\n");
        }
    }

    memset(slabclass, 0, sizeof(slabclass));

    while (++i < POWER_LARGEST && size <= settings.item_size_max / factor) {
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        slabclass[i].size = size;
        slabclass[i].perslab = settings.item_size_max / slabclass[i].size;
        size *= factor;

        if (settings.verbose > 1) {
            fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                    i, slabclass[i].size, slabclass[i].perslab);
        }
    }

    power_largest = i;
    slabclass[power_largest].size = settings.item_size_max;
    slabclass[power_largest].perslab = 1;
    if (settings.verbose > 1) {
        fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                i, slabclass[i].size, slabclass[i].perslab);
    }

    if (prealloc) {
        // slabs_preallocate(power_largest);
        printf("slabs_preallocate\n");
    }
}

static void *slab_maintenance_thread(void *arg) {
    printf("slab_maintenance_thread\n");
    return NULL;
}

static void *slab_rebalance_thread(void *arg) {
    printf("slab_rebalance_thread\n");
    return NULL;
}

int start_slab_maintenance_thread(void) {
    int ret;
    slab_rebalance_signal = 0;
    slab_rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env) {
        slab_bulk_check = atoi(env);
        if (slab_bulk_check == 0) {
            slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }

    if (pthread_cond_init(&slab_rebalance_cond, NULL) != 0) {
        fprintf(stderr, "Can't initialize rebalance condition\n");
        return -1;
    }
    pthread_mutex_init(&slabs_rebalance_lock, NULL);

    if ((ret = pthread_create(&maintenance_tid, NULL,
                    slab_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create slab maint thread: %s\n", strerror(ret));
        return -1;
    }

    if ((ret = pthread_create(&rebalance_tid, NULL,
                    slab_rebalance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
        return -1;
    }

    return 0;
}
