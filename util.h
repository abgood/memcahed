#ifndef MEMCACHED_UTIL_H
#define MEMCACHED_UTIL_H

bool safe_strtoul(const char *, uint32_t *);
void vperror(const char *, ...);

#endif
