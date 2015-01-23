#include "memcached.h"

#define hashsize(n) ((unsigned long int)1 << (n))
#define hashmask(n) (hashsize(n) - 1)

typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int sfd;
    enum conn_states init_state;
    int event_flags;
    int read_buffer_size;
    enum network_transport transport;
    CQ_ITEM *next;
};

typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
};

pthread_mutex_t cache_lock;
pthread_mutex_t worker_hang_lock;
pthread_mutex_t init_lock;
pthread_mutex_t cqi_freelist_lock;
pthread_cond_t init_cond;

static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t *item_locks;
static CQ_ITEM *cqi_freelist;
static LIBEVENT_THREAD *threads;
static LIBEVENT_DISPATCHER_THREAD dispatcher_thread;
static uint32_t item_lock_count;
static int init_count = 0;

unsigned int item_lock_hashpower;

void STATS_LOCK(void) {
    pthread_mutex_lock(&stats_lock);
}

void STATS_UNLOCK(void) {
    pthread_mutex_unlock(&stats_lock);
}

static void thread_libevent_process(int fd, short which, void *arg) {
    printf("thread_libevent_process\n");
}

static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

static void setup_thread(LIBEVENT_THREAD *me) {
    me->base = event_init();
    if (!me->base) {
        fprintf(stderr, "Can't allocate event base\n");
        exit(1);
    }

    event_set(&me->notify_event, me->notify_receive_fd,
            EV_READ | EV_PERSIST, thread_libevent_process, me);

    event_base_set(me->base, &me->notify_event);

    if (event_add(&me->notify_event, 0) == -1) {
        fprintf(stderr, "Can't monitor libevent notify pipe\n");
        exit(1);
    }

    me->new_conn_queue = malloc(sizeof(struct conn_queue));
    if (!me->new_conn_queue) {
        perror("Failed to allocate memory for connection queue");
        exit(EXIT_FAILURE);
    }
    cq_init(me->new_conn_queue);

    if (pthread_mutex_init(&me->stats.mutex, NULL) != 0) {
        perror("Failed to initialize mutex");
        exit(EXIT_FAILURE);
    }

    me->suffix_cache = cache_create("suffix", SUFFIX_SIZE, sizeof(char *), NULL, NULL);
    if (!me->suffix_cache) {
        fprintf(stderr, "Failed to create suffix cache\n");
        exit(EXIT_FAILURE);
    }
}

static void register_thread_initialized(void) {
    pthread_mutex_lock(&init_lock);
    init_count++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
    pthread_mutex_lock(&worker_hang_lock);
    pthread_mutex_unlock(&worker_hang_lock);
}

static void *worker_libevent(void *arg) {
    LIBEVENT_THREAD *me = arg;

    register_thread_initialized();

    event_base_loop(me->base, 0);
    return NULL;
}

static void create_worker(void *(*func)(void *), void *arg) {
    pthread_t thread;
    pthread_attr_t attr;
    int ret;

    pthread_attr_init(&attr);

    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        exit(1);
    }
}

static void wait_for_thread_registration(int nthreads) {
    while (init_count < nthreads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }
}

void memcached_thread_init(int nthreads, struct event_base *main_base) {
    int i;
    int power;

    pthread_mutex_init(&cache_lock, NULL);
    pthread_mutex_init(&worker_hang_lock, NULL);
    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);
    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;

    if (nthreads < 3) {
        power = 10;
    } else if (nthreads < 4) {
        power = 11;
    } else if (nthreads < 5) {
        power = 12;
    } else {
        power = 13;
    }

    if (power >= hashpower) {
        fprintf(stderr, "Hash table power size (%d) cannot be equal to or less than item lock table (%d)\n", hashpower, power);
        fprintf(stderr, "Item lock table grows with `-t N` (worker threadcount)\n");
        fprintf(stderr, "Hash table grows with `-o hashpower=N`\n");
        exit(1);
    }

    item_lock_count = hashsize(power);
    item_lock_hashpower = power;

    item_locks = calloc(item_lock_count, sizeof(pthread_mutex_t));
    if (!item_locks) {
        perror("Can't allocate item locks");
        exit(1);
    }

    // printf("%d\n", item_lock_count);
    for (i = 0; i < item_lock_count; i++) {
        pthread_mutex_init(&item_locks[i], NULL);
    }

    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (!threads) {
        perror("Can't allocate thread descriptors");
        exit(1);
    }

    dispatcher_thread.base = main_base;
    dispatcher_thread.thread_id = pthread_self();

    for (i = 0; i < nthreads; i++) {
        int fds[2];
        if (pipe(fds)) {
            perror("Can't create notify pipe");
            exit(1);
        }

        threads[i].notify_receive_fd = fds[0];
        threads[i].notify_send_fd = fds[1];

        setup_thread(&threads[i]);

        stats.reserved_fds += 5;
    }

    for (i = 0; i < nthreads; i++) {
        create_worker(worker_libevent, &threads[i]);
    }

    pthread_mutex_lock(&init_lock);
    wait_for_thread_registration(nthreads);
    pthread_mutex_unlock(&init_lock);
    // printf("%d\n", init_count);
}
