#ifndef MEMCACHED_ASSOC_H
#define MEMCACHED_ASSOC_H

extern unsigned int hashpower;

void assoc_init(const int);
int start_assoc_maintenance_thread(void);

#endif
