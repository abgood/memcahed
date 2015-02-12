#include "memcached.h"

struct settings settings;
struct stats stats;
time_t process_started;
conn **conns;
volatile int slab_rebalance_signal;
struct slab_rebalance slab_rebal;
volatile rel_time_t current_time;

static struct event_base *main_base;
static int max_fds;
static struct event clockevent;
static conn *listen_conn = NULL;
static volatile bool allow_new_conns = true;

static bool sanitycheck(void) {
    const char *ever = event_get_version();
    // current ever is 2.0.21-stable

    if (ever) {
        if (strncmp(ever, "1.", 2) == 0) {
            if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
                fprintf(stderr, "You are using libevent %s.\nPlease upgrade to"
                        " a more recent version (1.3 or newer)\n",
                        event_get_version());
                return false;
            }
        }
    }

    return true;
}

static void sig_handler(const int sig) {
    printf("Signal handled: %s.\n", strsignal(sig));
    exit(EXIT_SUCCESS);
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024;
    settings.maxconns = 1024;
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;
    settings.socketpath = NULL;
    settings.factor = 1.25;
    settings.chunk_size = 48;
    settings.num_threads = 4;
    settings.num_threads_per_udp = 0;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024;
    settings.maxconns_fast = false;
    settings.lru_crawler = false;
    settings.lru_crawler_sleep = 100;
    settings.lru_crawler_tocrawl = 0;
    settings.hashpower_init = 0;
    settings.slab_reassign = false;
    settings.slab_automove = 0;
    settings.shutdown_command = false;
    settings.tail_repair_time = TAIL_REPAIR_TIME_DEFAULT;
    settings.flush_enabled = true;
}

static void usage(void) {
    printf("-p <num>      TCP port number to listen on (default: 11211)\n"
           "-U <num>      UDP port number to listen on (default: 11211, 0 is off)\n"
           "-s <file>     UNIX socket path to listen on (disables network support)\n"
           "-A            enable ascii \"shutdown\" command\n"
           "-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
           "-l <addr>     interface to listen on (default: INADDR_ANY, all addresses)\n"
           "              <addr> may be specified as host:port. If you don't specify\n"
           "              a port number, the value you specified with -p or -U is\n"
           "              used. You may specify multiple addresses separated by comma\n"
           "              or by using -l multiple times\n"

           "-d            run as a daemon\n"
           "-r            maximize core file limit\n"
           "-u <username> assume identity of <username> (only when run as root)\n"
           "-m <num>      max memory to use for items in megabytes (default: 64 MB)\n"
           "-M            return error on memory exhausted (rather than removing items)\n"
           "-c <num>      max simultaneous connections (default: 1024)\n"
           "-k            lock down all paged memory.  Note that there is a\n"
           "              limit on how much memory you may lock.  Trying to\n"
           "              allocate more than that would fail, so be sure you\n"
           "              set the limit correctly for the user you started\n"
           "              the daemon with (not for -u <username> user;\n"
           "              under sh this is done with 'ulimit -S -l NUM_KB').\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           "-vv           very verbose (also print client commands/reponses)\n"
           "-vvv          extremely verbose (also print internal state transitions)\n"
           "-h            print this help and exit\n"
           "-i            print memcached and libevent license\n"
           "-V            print version and exit\n"
           "-P <file>     save PID in <file>, only used with -d option\n"
           "-f <factor>   chunk size growth factor (default: 1.25)\n"
           "-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
    printf("-L            Try to use large memory pages (if available). Increasing\n"
           "              the memory page size could reduce the number of TLB misses\n"
           "              and improve the performance. In order to get large pages\n"
           "              from the OS, memcached will allocate the total item-cache\n"
           "              in one large chunk.\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \":\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n");
    printf("-t <num>      number of threads to use (default: 4)\n");
    printf("-R            Maximum number of requests per event, limits the number of\n"
           "              requests process for a given connection to prevent \n"
           "              starvation (default: 20)\n");
    printf("-C            Disable use of CAS\n");
    printf("-b            Set the backlog queue limit (default: 1024)\n");
    printf("-B            Binding protocol - one of ascii, binary, or auto (default)\n");
    printf("-I            Override the size of each slab page. Adjusts max item size\n"
           "              (default: 1mb, min: 1k, max: 128m)\n");
#ifdef ENABLE_SASL
    printf("-S            Turn on Sasl authentication\n");
#endif
    printf("-F            Disable flush_all command\n");
    printf("-o            Comma separated list of extended or experimental options\n"
           "              - (EXPERIMENTAL) maxconns_fast: immediately close new\n"
           "                connections if over maxconns limit\n"
           "              - hashpower: An integer multiplier for how large the hash\n"
           "                table should be. Can be grown at runtime if not big enough.\n"
           "                Set this based on \"STAT hash_power_level\" before a \n"
           "                restart.\n"
           "              - tail_repair_time: Time in seconds that indicates how long to wait before\n"
           "                forcefully taking over the LRU tail item whose refcount has leaked.\n"
           "                The default is 3 hours.\n"
           "              - hash_algorithm: The hash table algorithm\n"
           "                default is jenkins hash. options: jenkins, murmur3\n"
           "              - lru_crawler: Enable LRU Crawler background thread\n"
           "              - lru_crawler_sleep: Microseconds to sleep between items\n"
           "                default is 100.\n"
           "              - lru_crawler_tocrawl: Max items to crawl per slab per run\n"
           "                default is 0 (unlimited)\n"
           );
    return;
}

static void usage_license(void) {
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    "\n"
    "\n"
    "This product includes software developed by Niels Provos.\n"
    "\n"
    "[ libevent ]\n"
    "\n"
    "Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "1. Redistributions of source code must retain the above copyright\n"
    "   notice, this list of conditions and the following disclaimer.\n"
    "2. Redistributions in binary form must reproduce the above copyright\n"
    "   notice, this list of conditions and the following disclaimer in the\n"
    "   documentation and/or other materials provided with the distribution.\n"
    "3. All advertising materials mentioning features or use of this software\n"
    "   must display the following acknowledgement:\n"
    "      This product includes software developed by Niels Provos.\n"
    "4. The name of the author may not be used to endorse or promote products\n"
    "   derived from this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
    "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
    "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
    "IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
    "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
    "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
    "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static int enable_large_pages(void) {
    return 0;
}

static void stats_init(void) {
    stats.curr_items = stats.total_items = stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
    stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = stats.evictions = stats.reclaimed = 0;
    stats.touch_cmds = stats.touch_misses = stats.touch_hits = stats.rejected_conns = 0;
    stats.malloc_fails = 0;
    stats.curr_bytes = stats.listen_disabled_num = 0;
    stats.hash_power_level = stats.hash_bytes = stats.hash_is_expanding = 0;
    stats.expired_unfetched = stats.evicted_unfetched = 0;
    stats.slabs_moved = 0;
    stats.accepting_conns = true;
    stats.slab_reassign_running = false;
    stats.lru_crawler_running = false;

    process_started = time(0) - ITEM_UPDATE_INTERVAL - 2;
    stats_prefix_init();
}

static void conn_init(void) {
    int next_fd = dup(1);
    int headroom = 10;
    struct rlimit rl;

    max_fds = settings.maxconns + headroom + next_fd;

    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        max_fds = rl.rlim_max;
    } else {
        fprintf(stderr, "Failed to query Maximum file descriptor; "
                "falling back to maxconns\n");
    }

    close(next_fd);

    if (!(conns = calloc(max_fds, sizeof(conn *)))) {
        fprintf(stderr, "Failed to allocate connection structures\n");
        exit(1);
    }
}

static void save_pid(const char *pid_file) {
    FILE *fp;
    if (access(pid_file, F_OK) == 0) {
        if ((fp = fopen(pid_file, "r"))) {
            char buffer[1024];
            if (fgets(buffer, sizeof(buffer), fp)) {
                unsigned int pid;
                if (safe_strtoul(buffer, &pid) && kill((pid_t)pid, 0) == 0) {
                    fprintf(stderr, "WARNING: The pid file contained the following (running) pid: %u\n", pid);
                }
            }
            fclose(fp);
        }
    }

    char tmp_pid_file[1024];
    snprintf(tmp_pid_file, sizeof(tmp_pid_file), "%s.tmp", pid_file);
    // printf("%s\n", tmp_pid_file);
    if (!(fp = fopen(tmp_pid_file, "w"))) {
        vperror("Could not open the pid file %s for writing", tmp_pid_file);
        return;
    }

    fprintf(fp, "%ld\n", (long)getpid());
    if (fclose(fp) == -1) {
        vperror("Could not close the pid file %s", tmp_pid_file);
    }

    if (rename(tmp_pid_file, pid_file) != 0) {
        vperror("Could not rename the pid file from %s to %s", tmp_pid_file, pid_file);
    }
}

static void clock_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;

    if (initialized) {
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    event_add(&clockevent, &t);

    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        current_time = (rel_time_t)(tv.tv_sec - process_started);
        // printf("%d\n", current_time);
    }
}

static int new_socket_unix(void) {
    int sfd;
    int flags;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
            fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }

    return sfd;
}

void conn_free(conn *c) {
    if (c) {
        assert(c);
        assert(c->sfd >= 0 && c->sfd < max_fds);

        MEMCACHED_CONN_DESTROY(c);

        conns[c->sfd] = NULL;
        if (c->hdrbuf) {
            free(c->hdrbuf);
        }
        if (c->msglist) {
            free(c->msglist);
        }
        if (c->rbuf) {
            free(c->rbuf);
        }
        if (c->wbuf) {
            free(c->wbuf);
        }
        if (c->ilist) {
            free(c->ilist);
        }
        if (c->suffixlist) {
            free(c->suffixlist);
        }
        if (c->iov) {
            free(c->iov);
        }
        free(c);
    }
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";

    switch (prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }

    return rv;
}

static void conn_close(conn *c) {
    assert(c);

    event_del(&c->event);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d connection closed.\n", c->sfd);
    }

    printf("conn_cleanup\n");

    MEMCACHED_CONN_RELEASE(c->sfd);
    printf("conn_set_state\n");
    close(c->sfd);

    pthread_mutex_lock(&conn_lock);
    allow_new_conns = true;
    pthread_mutex_unlock(&conn_lock);

    STATS_LOCK();
    stats.curr_conns--;
    STATS_UNLOCK();

    return;
}

void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        printf("aaa\n");
    }
}

static void conn_shrink(conn *c) {
    assert(c);

    if (IS_UDP(c->transport)) {
        return;
    }

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf) {
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);
        }

        newbuf = (char *)realloc((void *))
    }
}

static void reset_cmd_handler(conn *c) {
    c->cmd = -1;
    c->substate = bin_no_state;

    if (c->item) {
        item_remove(c->item);
        c->item = NULL;
    }

    conn_shrink(c);
}

static void drive_machine(conn *c) {
    bool stop = false;
    int sfd;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res = 0;
    const char *str;
    static int use_accept4 = 0;

    assert(c);

    while (!stop) {
        switch (c->state) {
        case conn_listening:
            addrlen = sizeof(addr);
            sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen);

            if (sfd == -1) {
                if (use_accept4 && errno == ENOSYS) {
                    use_accept4 = 0;
                    continue;
                }

                perror(use_accept4 ? "accept4()" : "accept()");
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    stop = true;
                } else if (errno == EMFILE) {
                    if (settings.verbose > 0) {
                        fprintf(stderr, "Too many open connections\n");
                    }

                    accept_new_conns(false);
                    stop = true;
                } else {
                    perror("accept()");
                    stop = true;
                }
                break;
            }

            if (!use_accept4) {
                if (fcntl(sfd, F_SETFL, fcntl(sfd, F_GETFL) | O_NONBLOCK) < 0) {
                    perror("settings O_NONBLOCK");
                    close(sfd);
                    break;
                }
            }

            if (settings.maxconns_fast &&
                stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1) {
                str = "ERROR Too many open connections\r\n";
                res = write(sfd, str, strlen(str));
                close(sfd);
                STATS_LOCK();
                stats.rejected_conns++;
                STATS_UNLOCK();
            } else {
                dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                        DATA_BUFFER_SIZE, tcp_transport);
            }
            stop = true;
            break;

        case conn_waiting:
            break;

        case conn_read:
            break;

        case conn_parse_cmd:
            break;

        case conn_new_cmd:
            --nreqs;
            printf("%d\n", nreqs);
            if (nreqs >= 0) {
                reset_cmd_handler(c);
            } else {
                printf("nreqs < 0\n");
                stop = true;
            }
            break;

        case conn_nread:
            break;

        case conn_swallow:
            break;

        case conn_write:
            break;

        case conn_mwrite:
            break;

        case conn_closing:
            break;

        case conn_closed:
            break;

        case conn_max_state:
            break;
        }
    }

    return;
}

void event_handler(const int fd, const short which, void *arg) {
    conn *c;

    c = (conn *)arg;
    assert(c);

    if (fd != c->sfd) {
        if (settings.verbose > 0) {
            fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        }

        conn_close(c);
        return;
    }

    drive_machine(c);

    return;
}

conn *conn_new(const int sfd, enum conn_states init_state,
        const int event_flags,
        const int read_buffer_size, enum network_transport transport,
        struct event_base *base) {
    conn *c;

    assert(sfd >= 0 && sfd < max_fds);
    c = conns[sfd];

    if (!c) {
        if (!(c = (conn *)calloc(1, sizeof(conn)))) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate connection object\n");
            return NULL;
        }
        MEMCACHED_CONN_CREATE(c);

        c->rbuf = c->wbuf = 0;
        c->ilist = 0;
        c->suffixlist = 0;
        c->iov = 0;
        c->msglist = 0;
        c->hdrbuf = 0;
        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->isize = ITEM_LIST_INITIAL;
        c->suffixsize = SUFFIX_LIST_INITIAL;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;
        c->hdrsize = 0;

        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->ilist = (item **)malloc(sizeof(item *) * c->isize);
        c->suffixlist = (char **)malloc(sizeof(char *) * c->suffixsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 ||
                c->msglist == 0 || c->suffixlist == 0) {
            conn_free(c);
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate buffers for connection\n");
            return NULL;
        }

        STATS_LOCK();
        stats.conn_structs++;
        STATS_UNLOCK();

        c->sfd = sfd;
        conns[sfd] = c;
    }

    c->transport = transport;
    c->protocol = settings.binding_protocol;

    if (!settings.socketpath) {
        c->request_addr_size = sizeof(c->request_addr);
    } else {
        c->request_addr_size = 0;
    }

    if (transport == tcp_transport && init_state == conn_new_cmd) {
        if (getpeername(sfd, (struct sockaddr *)&c->request_addr,
                    &c->request_addr_size)) {
            perror("getpeername");
            memset(&c->request_addr, 0, sizeof(c->request_addr));
        }
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            fprintf(stderr, "<%d server listening (%s)\n", sfd, prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            fprintf(stderr, "<%d server listening (udp)\n", sfd);
        } else if (c->protocol == negotiating_prot) {
            fprintf(stderr, "<%d new auto-negotiate client connection\n", sfd);
        } else if (c->protocol == ascii_prot) {
            fprintf(stderr, "<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            fprintf(stderr, "<%d new binary client connection.\n", sfd);
        } else {
            fprintf(stderr, "<%d new unknown (%d) client connection\n", sfd, c->protocol);
            assert(false);
        }
    }

    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->authenticated = false;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;
    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, 0) == -1) {
        fprintf(stderr, "event_add");
        return NULL;
    }

    STATS_LOCK();
    stats.curr_conns++;
    stats.total_conns++;
    STATS_UNLOCK();

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    return c;
}

static int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags = 1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode)) {
            unlink(path);
        }
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask(~(access_mask & 0777));

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind()");
        close(sfd);
        umask(old_umask);
        return 1;
    }

    umask(old_umask);
    if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        close(sfd);
        return 1;
    }

    if (!(listen_conn = conn_new(sfd, conn_listening,
                    EV_READ | EV_PERSIST, 1,
                    local_transport, main_base))) {
        fprintf(stderr, "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}

static int new_socket(struct addrinfo *ai) {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
            fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }

    return sfd;
}

static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;
    
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0) {
            perror("getsockopt(SO_SNDBUF)");
        }

        return;
    }

    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
    }
}

static int server_socket(const char *interface,
        int port,
        enum network_transport transport,
        FILE *portnumber_file) {
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                             .ai_family = AF_UNSPEC};
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags = 1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

    if (port == -1) {
        port = 0;
    }

    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error = getaddrinfo(interface, port_buf, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM) {
            fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        } else {
            perror("getaddrinfo()");
        }
        return 1;
    }

    for (next = ai; next; next = next->ai_next) {
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == -1) {
            if (errno == EMFILE) {
                perror("server_socket");
                exit(EX_OSERR);
            }
            continue;
        }

        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *)&flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                close(sfd);
                continue;
            }
        }

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
            }

            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0) {
                perror("setsockopt");
            }

            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
            }
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }

            close(sfd);
            continue;
        } else {
            success++;
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
                perror("listen()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }

            if (portnumber_file &&
                    (next->ai_addr->sa_family == AF_INET ||
                     next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;

                socklen_t len = sizeof(my_sockaddr);

                if (getsockname(sfd, (struct sockaddr *)&my_sockaddr, &len) == 0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (IS_UDP(transport)) {
            // printf("udp transport\n");
            int c;

            for (c = 0; c < settings.num_threads_per_udp; c++) {
                int per_thread_fd = c ? dup(sfd) : sfd;
                dispatch_conn_new(per_thread_fd, conn_read,
                        EV_READ | EV_PERSIST,
                        UDP_READ_BUFFER_SIZE, transport);
            }
        } else {
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                            EV_READ | EV_PERSIST, 1,
                            transport, main_base))) {
                fprintf(stderr, "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }

            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
        }
    }

    freeaddrinfo(ai);

    return success == 0;
}

static int server_sockets(int port, enum network_transport transport,
        FILE *portnumber_file) {
    if (!settings.inter) {
        return server_socket(settings.inter, port, transport, portnumber_file);
    } else {
        char *b;
        char *p;
        int ret = 0;
        char *list = strdup(settings.inter);

        if (!list) {
            fprintf(stderr, "Failed to allocate memory for parsing server interface string\n");
            return 1;
        }

        for (p = strtok_r(list, ";,", &b);
                p != NULL;
                p = strtok_r(NULL, ";,", &b)) {
            int the_port = port;
            char *s = strchr(p, ':');

            if (s) {
                *s = '\0';
                ++s;
                if (!safe_strtol(s, &the_port)) {
                    fprintf(stderr, "Invalid port number: \"%s\"", s);
                    return 1;
                }
            }

            if (strcmp(p, "*") == 0) {
                p = NULL;
            }

            ret |= server_socket(p, the_port, transport, portnumber_file);
        }

        free(list);
        return ret;
    }
}

int main (int argc, char **argv) {
    uint32_t tocrawl;
    int c;
    int maxcore = 0;
    int size_max = 0;
    int retval = EXIT_SUCCESS;

    bool tcp_specified = false;
    bool udp_specified = false;
    bool lock_memory = false;
    bool do_daemonize = false;
    bool preallocate = false;
    bool protocol_specified = false;

    char *username = NULL;
    char *pid_file = NULL;
    char *buf;
    char *subopts;
    char *subopts_value;
    char unit = '\0';

    enum hashfunc_type hash_type = JENKINS_HASH;
    struct rlimit rlim;
    struct passwd *pw;

    enum {
        MAXCONNS_FAST = 0,
        HASHPOWER_INIT,
        SLAB_REASSIGN,
        SLAB_AUTOMOVE,
        TAIL_REPAIR_TIME,
        HASH_ALGORITHM,
        LRU_CRAWLER,
        LRU_CRAWLER_SLEEP,
        LRU_CRAWLER_TOCRAWL
    };

    char *const subopts_tokens[] = {
        [MAXCONNS_FAST] = "maxconns_fast",
        [HASHPOWER_INIT] = "hashpower",
        [SLAB_REASSIGN] = "slab_reassign",
        [SLAB_AUTOMOVE] = "slab_automove",
        [TAIL_REPAIR_TIME] = "tail_repair_time",
        [HASH_ALGORITHM] = "hash_algorithm",
        [LRU_CRAWLER] = "lru_crawler",
        [LRU_CRAWLER_SLEEP] = "lru_crawler_sleep",
        [LRU_CRAWLER_TOCRAWL] = "lru_crawler_tocrawl",
        NULL
    };

    if (!sanitycheck()) {
        return EX_OSERR;
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    settings_init();

    init_lru_crawler();

    setbuf(stderr, NULL);

    while (-1 != (c = getopt(argc, argv,
        "a:"
        "A" 
        "p:"
        "s:"
        "U:"
        "m:"
        "M" 
        "c:"
        "k" 
        "hiV"
        "r" 
        "v" 
        "d" 
        "l:"
        "u:"
        "P:"
        "f:"
        "n:"
        "t:"
        "D:"
        "L" 
        "R:"
        "C" 
        "b:"
        "B:"
        "I:"
        "S" 
        "F" 
        "o:"
        ))) {
        switch (c) {
        case 'A':
            settings.shutdown_command = true;
            break;

        case 'a':
            settings.access = strtol(optarg, NULL, 8);
            break;

        case 'U':
            settings.udpport = atoi(optarg);
            udp_specified = true;
            break;

        case 'p':
            settings.port = atoi(optarg);
            tcp_specified = true;
            break;

        case 's':
            settings.socketpath = optarg;
            break;

        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
            break;

        case 'M':
            settings.evict_to_free = 0;
            break;

        case 'c':
            settings.maxconns = atoi(optarg);
            break;

        case 'h':
            usage();
            exit(EXIT_SUCCESS);

        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);

        case 'V':
            printf("1.4.22\n");
            exit(EXIT_SUCCESS);

        case 'k':
            lock_memory = true;
            break;

        case 'v':
            settings.verbose++;
            break;

        case 'l':
            if (settings.inter) {
                size_t len = strlen(settings.inter) + strlen(optarg) + 2;
                char *p = malloc(len);
                if (!p) {
                    fprintf(stderr, "Failed to allocate memory\n");
                    return 1;
                }

                snprintf(p, len, "%s,%s", settings.inter, optarg);
                free(settings.inter);
                settings.inter = p;
            } else {
                settings.inter = strdup(optarg);
            }
            break;

        case 'd':
            do_daemonize = true;
            break;

        case 'r':
            maxcore = 1;
            break;

        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event == 0) {
                fprintf(stderr, "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;

        case 'u':
            username = optarg;
            break;

        case 'P':
            pid_file = optarg;
            break;

        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                fprintf(stderr, "Factor must be greater than 1\n");
                return 1;
            }
            break;

        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                fprintf(stderr, "Chunk size must be greater than 0\n");
                return 1;
            }
            break;

        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                fprintf(stderr, "Number of threads must be greater than 0\n");
                return 1;
            }

            if (settings.num_threads > 64) {
                fprintf(stderr, "WARNING: setting a high number of worker"
                        " threads is not recommended.\n"
                        "Set this value to the number of cores in your"
                        " machine or less.\n");
            }
            break;

        case 'D':
            if (!optarg || !optarg[0]) {
                fprintf(stderr, "No delimiter specified\n");
                return 1;
            }

            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;

        case 'L':
            if (enable_large_pages() == 0) {
                preallocate = true;
            } else {
                fprintf(stderr, "Can't enable large pages on this systems\n"
                        "(There is no Linux support as of this version)\n");
                return 1;
            }
            break;

        case 'C':
            settings.use_cas = false;
            break;

        case 'b':
            settings.backlog = atoi(optarg);
            break;

        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                fprintf(stderr, "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;

        case 'I':
            buf = strdup(optarg);
            unit = buf[strlen(buf) - 1];
            if (unit == 'k' || unit == 'm' ||
                    unit == 'K' || unit == 'M') {
                buf[strlen(buf) - 1] = '\0';
                size_max = atoi(buf);
                if (unit == 'k' || unit == 'K') {
                    size_max *= 1024;
                }
                if (unit == 'm' || unit == 'M') {
                    size_max *= 1024 * 1024;
                }
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(buf);
            }

            if (settings.item_size_max < 1024) {
                fprintf(stderr, "Item max size cannot be less than 1024 bytes.\n");
                return 1;
            }

            if (settings.item_size_max > 1024 * 1024 * 128) {
                fprintf(stderr, "Can't set item size limit higher than 128 mb.\n");
                return 1;
            }

            if (settings.item_size_max > 1024 * 1024) {
                fprintf(stderr, "WARNING: Setting item max size above 1MB is not"
                        " recommended!\n"
                        "Raising this limit increases the minimum memory requirements\n"
                        " and will decrease your memory efficiency.\n");
            }

            free(buf);
            break;

        case 'S':
            settings.sasl = true;
            break;

        case 'F':
            settings.flush_enabled = false;
            break;

        case 'o':
            subopts = optarg;

            while (*subopts != '\0') {
                switch (getsubopt(&subopts, subopts_tokens, &subopts_value)) {
                case MAXCONNS_FAST:
                    settings.maxconns_fast = true;
                    break;
                case HASHPOWER_INIT:
                    if (!subopts_value) {
                        fprintf(stderr, "Missing numeric argument for hashpower\n");
                        return 1;
                    }
                    settings.hashpower_init = atoi(subopts_value);
                    if (settings.hashpower_init < 12) {
                        fprintf(stderr, "Initial hashtable multiplier of %d is too low\n", settings.hashpower_init);
                        return 1;
                    } else if (settings.hashpower_init > 64) {
                        fprintf(stderr, "Initial hashtable multiplier of %d is too high\n"
                                "Choose a value based on \"STAT hash_power_level\" from a running instance\n", settings.hashpower_init);
                        return 1;
                    }
                    break;
                case SLAB_REASSIGN:
                    settings.slab_reassign = true;
                    break;
                case SLAB_AUTOMOVE:
                    if (!subopts_value) {
                        settings.slab_automove = 1;
                        break;
                    }
                    settings.slab_automove = atoi(subopts_value);
                    if (settings.slab_automove < 0 || settings.slab_automove > 2) {
                        fprintf(stderr, "slab_automove must be between 0 and 2\n");
                        return 1;
                    }
                    break;
                case TAIL_REPAIR_TIME:
                    if (!subopts_value) {
                        fprintf(stderr, "Missing numeric argument for tail_repair_time\n");
                        return 1;
                    }
                    settings.tail_repair_time = atoi(subopts_value);
                    if (settings.tail_repair_time < 10) {
                        fprintf(stderr, "Can't set tail_repair_time to less than 10 seconds\n");
                        return 1;
                    }
                    break;
                case HASH_ALGORITHM:
                    if (!subopts_value) {
                        fprintf(stderr, "Missing hash_algorithm argument\n");
                        return 1;
                    }
                    if (strcmp(subopts_value, "jenkins") == 0) {
                        hash_type = JENKINS_HASH;
                    } else if (strcmp(subopts_value, "murmur3") == 0) {
                        hash_type = MURMUR3_HASH;
                    } else {
                        fprintf(stderr, "Unknown hash_algorithm option(jenkins, murmur3)\n");
                        return 1;
                    }
                    break;
                case LRU_CRAWLER:
                    if (start_item_crawler_thread() != 0) {
                        fprintf(stderr, "Failed to enable LRU crawler thread\n");
                        return 1;
                    }
                    break;
                case LRU_CRAWLER_SLEEP:
                    settings.lru_crawler_sleep = atoi(subopts_value);
                    if (settings.lru_crawler_sleep > 1000000 || settings.lru_crawler_sleep < 0) {
                        fprintf(stderr, "LRU crawler sleep must be between 0 and 1 seconds\n");
                        return 1;
                    }
                    break;
                case LRU_CRAWLER_TOCRAWL:
                    if (!safe_strtoul(subopts_value, &tocrawl)) {
                        fprintf(stderr, "lru_crawler_tocrawl takes a numeric 32bit value\n");
                        return 1;
                    }
                    settings.lru_crawler_tocrawl = tocrawl;
                    break;
                default:
                    printf("Illegal subopts \"%s\"\n", subopts_value);
                    return 1;
                }
            }

            break;

        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    if (hash_init(hash_type) != 0) {
        fprintf(stderr, "Failed to initialize hash_algorithm!\n");
        exit(EX_USAGE);
    }

    if (settings.inter && strchr(settings.inter, ',')) {
        settings.num_threads_per_udp = 1;
    } else {
        settings.num_threads_per_udp = settings.num_threads;
    }

    if (settings.sasl) {
        if (!protocol_specified) {
            settings.binding_protocol = binary_prot;
        } else {
            if (settings.binding_protocol != binary_prot) {
                fprintf(stderr, "ERROR: You cannot allow the ASCII protocol while using SASL.\n");
                exit(EX_USAGE);
            }
        }
    }

    if (tcp_specified && !udp_specified) {
        settings.udpport = settings.port;
    } else if (udp_specified && !tcp_specified) {
        settings.port = settings.udpport;
    }

    if (maxcore != 0) {
        struct rlimit rlim_new;

        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            fprintf(stderr, "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }

    /*
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        rlim.rlim_cur = settings.maxconns;
        rlim.rlim_max = settings.maxconns;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            fprintf(stderr, "failed to set rlimit for open files. Try starting as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }
    */

    if (getuid() == 0 || geteuid() == 0) {
        if (username == 0 || *username == '\0') {
            fprintf(stderr, "can't run as root without the -u switch\n");
            exit(EX_USAGE);
        }
        if ((pw = getpwnam(username)) == 0) {
            fprintf(stderr, "can't fnd the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "failed to assume identity of user %s\n", username);
            exit(EX_OSERR);
        }
    }

    if (settings.sasl) {
        init_sasl();
    }

    if (do_daemonize) {
        if (sigignore(SIGHUP) == -1) {
            perror("Failed to ignore SIGHUP");
        }
        if (daemonize(maxcore, settings.verbose) == -1) {
            fprintf(stderr, "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }

    if (lock_memory) {
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            fprintf(stderr, "warning: -k invalid, mlockall() failed: %s\n", strerror(errno));
        }
    }

    main_base = event_init();

    stats_init();

    assoc_init(settings.hashpower_init);

    conn_init();

    slabs_init(settings.maxbytes, settings.factor, preallocate);

    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }

    memcached_thread_init(settings.num_threads, main_base);

    if (start_assoc_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }

    if (settings.slab_reassign && start_slab_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }

    clock_handler(0, 0, 0);

    if (settings.socketpath) {
        errno = 0;
        if (server_socket_unix(settings.socketpath, settings.access)) {
            vperror("failed to listen on UNIX socket: %s", settings.socketpath);
            exit(EX_OSERR);
        }
    }

    if (!settings.socketpath) {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        char temp_portnumber_filename[PATH_MAX];
        FILE *portnumber_file = NULL;

        if (portnumber_filename) {
            snprintf(temp_portnumber_filename,
                    sizeof(temp_portnumber_filename),
                    "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (!portnumber_file) {
                fprintf(stderr, "Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        errno = 0;
        if (settings.port && server_sockets(settings.port, tcp_transport,
                    portnumber_file)) {
            vperror("failed to listen to TCP port %d", settings.port);
            exit(EX_OSERR);
        }

        errno = 0;
        if (settings.udpport && server_sockets(settings.udpport, udp_transport,
                    portnumber_file)) {
            vperror("failed to listen on UDP port %d", settings.udpport);
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

    usleep(2000);

    if (stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1) {
        fprintf(stderr, "Maxconns setting is too low, use -c to increase.\n");
        exit(EXIT_FAILURE);
    }

    if (pid_file) {
        save_pid(pid_file);
    }

    drop_privileges();

    if (event_base_loop(main_base, 0) != 0) {
        retval = EXIT_FAILURE;
    }

    printf("memcached done\n");
    return retval;
}
