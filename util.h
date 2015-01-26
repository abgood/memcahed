#ifndef MEMCACHED_UTIL_H
#define MEMCACHED_UTIL_H

bool safe_strtoul(const char *, uint32_t *);
bool safe_strtol(const char *str, int32_t *);
void vperror(const char *, ...);

#endif
