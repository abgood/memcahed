#include "memcached.h"

#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    assert(out);
    assert(str);
    *out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if ((errno == ERANGE) || (str == endptr)) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long)l < 0) {
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }

        *out = l;
        return true;
    }

    return false;
}

void vperror(const char *fmt, ...) {
    int old_errno = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);

    if (vsnprintf(buf, sizeof(buf), fmt, ap) == -1) {
        buf[sizeof(buf) - 1] = '\0';
    }

    va_end(ap);
    errno = old_errno;
    perror(buf);
}
