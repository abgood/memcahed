#include "memcached.h"

static int lru_crawler_initialized = 0;
static volatile int do_run_lru_crawler_thread = 0;
static pthread_cond_t lru_crawler_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t lru_crawler_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t item_crawler_tid;

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
