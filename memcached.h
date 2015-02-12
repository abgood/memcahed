#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sysexits.h>
#include <stdbool.h>
#include <event.h>
#include <ctype.h>
#include <signal.h>
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <pwd.h>
#include <fcntl.h>
#include <event.h>
#include <limits.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sasl/sasl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "items.h"
#include "hash.h"
#include "util.h"
#include "jenkins_hash.h"
#include "murmur3_hash.h"
#include "sasl_defs.h"
#include "stats.h"
#include "assoc.h"
#include "slabs.h"
#include "cache.h"
#include "trace.h"

#define ITEM_UPDATE_INTERVAL 60
#define HASHPOWER_DEFAULT 16
#define POWER_SMALLEST 1
#define POWER_LARGEST 200
#define CHUNK_ALIGN_BYTES 8
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)
#define SUFFIX_SIZE 24
#define DATA_BUFFER_SIZE 2048
#define ITEM_LIST_INITIAL 200
#define SUFFIX_LIST_INITIAL 20
#define IOV_LIST_INITIAL 400
#define MSG_LIST_INITIAL 10
#define IS_UDP(x) (x == udp_transport)
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
#define UDP_READ_BUFFER_SIZE 65536
#define TAIL_REPAIR_TIME_DEFAULT 0
#define ITEM_LINKED 1
#define ITEM_CAS 2
#define mutex_unlock(x) pthread_mutex_unlock(x)
#define ITEM_SLABBED 4
#define READ_BUFFER_HIGHWAT 8192

#define ITEM_key(item) (((char *)&((item)->data)) \
        + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nkey + 1 \
        + (item)->nsuffix + (item)->nbytes \
        + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

typedef unsigned int rel_time_t;

enum protocol {
    ascii_prot = 3,
    binary_prot,
    negotiating_prot
};

enum conn_states {
    conn_listening,
    conn_new_cmd,
    conn_waiting,
    conn_read,
    conn_parse_cmd,
    conn_write,
    conn_nread,
    conn_swallow,
    conn_closing,
    conn_mwrite,
    conn_closed,
    conn_max_state
};

enum network_transport {
    local_transport,
    tcp_transport,
    udp_transport
};

enum bin_substates {
    bin_no_state,
    bin_reading_set_header,
    bin_reading_cas_header,
    bin_read_set_value,
    bin_reading_get_key,
    bin_reading_stat,
    bin_reading_del_header,
    bin_reading_incr_header,
    bin_read_flush_exptime,
    bin_reading_sasl_auth,
    bin_reading_sasl_auth_data,
    bin_reading_touch_key,
};

struct settings {
    bool shutdown_command;
    bool use_cas;
    bool sasl;
    bool flush_enabled;
    bool maxconns_fast;
    bool slab_reassign;
    bool lru_crawler;

    size_t maxbytes;
    double factor;
    enum protocol binding_protocol;
    uint32_t lru_crawler_tocrawl;
    rel_time_t oldest_live;

    int access;
    int maxconns;
    int evict_to_free;
    int udpport;
    int port;
    int verbose;
    int reqs_per_event;
    int chunk_size;
    int num_threads;
    int detail_enabled;
    int backlog;
    int item_size_max;
    int hashpower_init;
    int slab_automove;
    int tail_repair_time;
    int lru_crawler_sleep;
    int num_threads_per_udp;

    char *socketpath;
    char *inter;
    char *hash_algorithm;
    char prefix_delimiter;
};

struct stats {
    unsigned int curr_items;
    unsigned int total_items;
    unsigned int curr_conns;
    unsigned int total_conns;
    unsigned int conn_structs;
    unsigned int hash_power_level;
    unsigned int hash_bytes;
    unsigned int hash_is_expanding;
    unsigned int reserved_fds;

    uint64_t get_cmds;
    uint64_t set_cmds;
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t evictions;
    uint64_t reclaimed;
    uint64_t touch_cmds;
    uint64_t touch_misses;
    uint64_t touch_hits;
    uint64_t rejected_conns;
    uint64_t malloc_fails;
    uint64_t curr_bytes;
    uint64_t listen_disabled_num;
    uint64_t expired_unfetched;
    uint64_t evicted_unfetched;
    uint64_t slabs_moved;

    bool accepting_conns;
    bool slab_reassign_running;
    bool lru_crawler_running;
};

typedef struct _stritem {
    struct _stritem *next;
    struct _stritem *prev;
    struct _stritem *h_next;

    rel_time_t time;
    rel_time_t exptime;

    int nbytes;
    unsigned short refcount;
    uint8_t nsuffix;
    uint8_t it_flags;
    uint8_t slabs_clsid;
    uint8_t nkey;

    union {
        uint64_t cas;
        char end;
    } data[];
} item;

struct slab_stats {
    uint64_t set_cmds;
    uint64_t get_hits;
    uint64_t touch_hits;
    uint64_t delete_hits;
    uint64_t cas_hits;
    uint64_t cas_badval;
    uint64_t incr_hits;
    uint64_t decr_hits;
};

struct thread_stats {
    pthread_mutex_t mutex;
    uint64_t get_cmds;
    uint64_t get_misses;
    uint64_t touch_cmds;
    uint64_t touch_misses;
    uint64_t delete_misses;
    uint64_t incr_misses;
    uint64_t decr_misses;
    uint64_t cas_misses;
    uint64_t bytes_read;
    uint64_t bytes_written;
    uint64_t flush_cmds;
    uint64_t conn_yields;
    uint64_t auth_cmds;
    uint64_t auth_errors;
    struct slab_stats slab_stats[MAX_NUMBER_OF_SLAB_CLASSES];
};

typedef struct {
    pthread_t thread_id;
    struct event_base *base;
    struct event notify_event;
    int notify_receive_fd;
    int notify_send_fd;
    struct thread_stats stats;
    struct conn_queue *new_conn_queue;
    cache_t *suffix_cache;
} LIBEVENT_THREAD;

typedef struct conn conn;
struct conn {
    int sfd;
    int rsize;
    int wsize;
    int isize;
    int suffixsize;
    int iovsize;
    int msgsize;
    int hdrsize;
    int rlbytes;
    int rbytes;
    int wbytes;
    int ileft;
    int suffixleft;
    int iovused;
    int msgcurr;
    int msgused;

    char *rbuf;
    char *wbuf;
    char *rcurr;
    char *wcurr;
    char *ritem;

    char **suffixlist;
    char **suffixcurr;
    unsigned char *hdrbuf;

    item **ilist;
    item **icurr;

    bool authenticated;
    bool noreply;

    socklen_t request_addr_size;

    short cmd;
    short ev_flags;

    void *write_and_free;
    void *item;

    struct iovec *iov;
    struct msghdr *msglist;
    struct sockaddr_in6 request_addr;
    struct event event;

    enum protocol protocol;
    enum network_transport transport;
    enum conn_states state;
    enum conn_states write_and_go;
    enum bin_substates substate;

    conn *next;
    LIBEVENT_THREAD *thread;
};

typedef struct {
    pthread_t thread_id;
    struct event_base *base;
} LIBEVENT_DISPATCHER_THREAD;

struct slab_rebalance {
    void *slab_start;
    void *slab_end;
    void *slab_pos;
    int s_clsid;
    int d_clsid;
    int busy_items;
    uint8_t done;
};

extern struct settings settings;
extern struct stats stats;
extern volatile int slab_rebalance_signal;
extern struct slab_rebalance slab_rebal;
extern pthread_mutex_t conn_lock;

int daemonize(int, int);
int sigignore(int);
void STATS_LOCK(void);
void STATS_UNLOCK(void);
void memcached_thread_init(int, struct event_base *);
void dispatch_conn_new(int, enum conn_states, int, int, enum network_transport);
conn *conn_new(const int, enum conn_states, const int, const int, enum network_transport, struct event_base *);
void drop_privileges(void);
void accept_new_conns(const bool);
void do_accept_new_conns(const bool);
void item_remove(item *);
void do_item_remove(item *it);
unsigned short refcount_decr(unsigned short *);

static inline int mutex_lock(pthread_mutex_t *mutex) {
    while (pthread_mutex_trylock(mutex));
    return 0;
}

#endif
