#include "memcached.h"
uint32_t jenkins_hash(const void *, size_t);

int hash_init(enum hashfunc_type type) {
    switch (type) {
        case JENKINS_HASH:
            hash = jenkins_hash;
            settings.hash_algorithm = "jenkins";
            break;
        case MURMUR3_HASH:
            // hash = MurmurHash3_x86_32;
            settings.hash_algorithm = "murmur3";
            break;
        default:
            return -1;
    }
    return 0;
}
