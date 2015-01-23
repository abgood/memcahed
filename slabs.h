#ifndef MEMCACHED_SLABS_H
#define MEMCACHED_SLABS_H

void slabs_init(const size_t, const double, const bool);
int start_slab_maintenance_thread(void);

#endif
