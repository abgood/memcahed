#include "memcached.h"

typedef unsigned long int ub4;

unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((ub4)1 << (n))
#define DEFAULT_HASH_BULK_MOVE 1

static item **primary_hashtable = 0;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_t maintenance_tid;
static volatile int do_run_maintenance_thread = 1;
static bool expanding = false;
static bool started_expanding = false;

int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

void assoc_init(const int hashtable_init) {
    if (hashtable_init) {
        hashpower = hashtable_init;
    }

    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (!primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }

    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

static void *assoc_maintenance_thread(void *arg) {
    mutex_lock(&maintenance_lock);

    while (do_run_maintenance_thread) {
        int ii = 0;

        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            printf("hash_bulk_move expanding\n");
        }

        if (!expanding) {
            started_expanding = false;
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            printf("pause_threads\n");
        }
    }

    return NULL;
}

int start_assoc_maintenance_thread(void) {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }

    pthread_mutex_init(&maintenance_lock, NULL);
    if ((ret = pthread_create(&maintenance_tid, NULL,
                    assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }

    return 0;
}
