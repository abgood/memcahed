#include "memcached.h"

#define LARGEST_ID POWER_LARGEST
#define DEBUG_REFCNT(it, op) while (0)

static int lru_crawler_initialized = 0;
static volatile int do_run_lru_crawler_thread = 0;
static pthread_cond_t lru_crawler_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t lru_crawler_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t item_crawler_tid;
static item *heads[LARGEST_ID];
static item *tails[LARGEST_ID];

int init_lru_crawler(void) {
    if (lru_crawler_initialized == 0) {
        if (pthread_cond_init(&lru_crawler_cond, NULL) != 0) {
            fprintf(stderr, "Can't initialize lru crawler condition\n");
            return -1;
        }

        pthread_mutex_init(&lru_crawler_lock, NULL);
        lru_crawler_initialized = 1;
    }

    return 0;
}

static void *item_crawler_thread(void *arg) {
    printf("item_crawler_thread\n");
    return (void *)0;
}

int start_item_crawler_thread(void) {
    int ret;

    if (settings.lru_crawler) {
        return -1;
    }

    pthread_mutex_lock(&lru_crawler_lock);
    do_run_lru_crawler_thread = 1;
    settings.lru_crawler = true;
    if ((ret = pthread_create(&item_crawler_tid, NULL, item_crawler_thread, NULL)) != 0) {
        fprintf(stderr, "can't create LRU crawler thread: %s\n", strerror(ret));
        pthread_mutex_unlock(&lru_crawler_lock);
        return -1;
    }
    pthread_mutex_unlock(&lru_crawler_lock);

    return 0;
}

void item_free(item *it) {
    size_t ntotal = ITEM_ntotal(it);
    unsigned int clsid;

    assert((it->it_flags & ITEM_LINKED) == 0);
    assert(it != heads[it->slabs_clsid]);
    assert(it != tails[it->slabs_clsid]);
    assert(it->refcount == 0);

    clsid = it->slabs_clsid;
    it->slabs_clsid = 0;
    DEBUG_REFCNT(it, 'F');
    // slabs_free(it, ntotal, clsid);
    printf("slabs_free: %d, %d\n", clsid, (int)ntotal);
}

void do_item_remove(item *it) {
    MEMCACHED_ITEM_REMOVE(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);
    assert(it->refcount > 0);

    if (refcount_decr(&it->refcount) == 0) {
        item_free(it);
    }
}

item *do_item_get(const char *key, const size_t nkey, const uint32_t hv) {
    item *it = assoc_find(key, nkey, hv);
}
