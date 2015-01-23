#include "memcached.h"

#define ENABLE_SASL_PWDB
#define HAVE_SASL_CB_GETCONF

char my_sasl_hostname[1025];

#ifndef HAVE_SASL_CALLBACK_FT
typedef int (*sasl_callback_ft)(void);
#endif

/// ENABLE_SASL_PWDB
#ifdef ENABLE_SASL_PWDB
#define MAX_ENTRY_LEN 256
static const char *memcached_sasl_pwdb;

static int sasl_server_userdb_checkpass(sasl_conn_t *conn,
        void *context,
        const char *user,
        const char *pass,
        unsigned passlen,
        struct propctx *propctx) {
    printf("sasl_server_userdb_checkpass\n");
    return 0;
}
#endif
/// ENABLE_SASL_PWDB


/*
/// HAVE_SASL_CB_GETCONF
#ifdef HAVE_SASL_CB_GETCONF
static int sasl_getconf(void *context, const char **path) {
    printf("sasl_getconf\n");
    return 0;
}
#endif
/// HAVE_SASL_CB_GETCONF
*/

static int sasl_log(void *context, int level, const char *message) {
    printf("sasl_log\n");
    return 0;
}



static sasl_callback_t sasl_callbacks[] = {
#ifdef ENABLE_SASL_PWDB
    {SASL_CB_SERVER_USERDB_CHECKPASS, (sasl_callback_ft)sasl_server_userdb_checkpass, NULL},
#endif
    {SASL_CB_LOG, (sasl_callback_ft)sasl_log, NULL},
#ifdef HAVE_SASL_CB_GETCONF
    // {SASL_CB_GETCONFPATH, sasl_getconf, NULL},
#endif
    {SASL_CB_LIST_END, NULL, NULL}
};

void init_sasl(void) {
#ifdef ENABLE_SASL_PWDB
    memcached_sasl_pwdb = getenv("MEMCACHED_SASL_PWDB");
    if (!memcached_sasl_pwdb) {
        if (settings.verbose) {
            fprintf(stderr,
                    "INFO: MEMCACHED_SASL_PWDB not specified. "
                    "Internal passwd database disabled\n");
        }

        sasl_callbacks[0].id = SASL_CB_LIST_END;
        sasl_callbacks[0].proc = NULL;
    }
#endif

    memset(my_sasl_hostname, 0, sizeof(my_sasl_hostname));
    if (gethostname(my_sasl_hostname, sizeof(my_sasl_hostname) - 1) == -1) {
        if (settings.verbose) {
            fprintf(stderr, "Error discovering hostname for SASL\n");
        }
        my_sasl_hostname[0] = '\0';
    }

    if (sasl_server_init(sasl_callbacks, "memcached") != SASL_OK) {
        fprintf(stderr, "Error initializing sasl.\n");
        exit(EXIT_FAILURE);
    } else {
        if (settings.verbose) {
            fprintf(stderr, "Initialized SASL.\n");
        }
    }
}
