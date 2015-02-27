#include "memcached.h"

#define rot(x, k) (((x) << (k)) ^ ((x) >> (32 - (k))))

#define mix(a, b, c) { \
    a -= c; a ^= rot(c, 4); c += b; \
    b -= a; b ^= rot(a, 6); a += c; \
    c -= b; c ^= rot(b, 8); b += a; \
    a -= c; a ^= rot(c,16); c += b; \
    b -= a; b ^= rot(a,19); a += c; \
    c -= b; c ^= rot(b, 4); b += a; \
}

#define final(a, b, c) { \
    c ^= b; c -= rot(b,14); \
    a ^= c; a -= rot(c,11); \
    b ^= a; b -= rot(a,25); \
    c ^= b; c -= rot(b,16); \
    a ^= c; a -= rot(c,4);  \
    b ^= a; b -= rot(a,14); \
    c ^= b; c -= rot(b,24); \
}

uint32_t jenkins_hash(const void *key, size_t length) {
    uint32_t a, b, c;
    union { const void *ptr; size_t i; } u;

    a = b = c = 0xdeadbeef + ((uint32_t)length) + 0;

    u.ptr = key;

    /* little endian mode */
    const uint8_t *k = key;

    while (length > 12) {
        a += ((uint32_t)k[0] << 24);
        a += ((uint32_t)k[1] << 16);
        a += ((uint32_t)k[2] << 8);
        a += ((uint32_t)k[3]);
        b += ((uint32_t)k[4] << 24);
        b += ((uint32_t)k[5] << 16);
        b += ((uint32_t)k[6] << 8);
        b += ((uint32_t)k[7]);
        c += ((uint32_t)k[8] << 24);
        c += ((uint32_t)k[9] << 16);
        c += ((uint32_t)k[10] << 8);
        c += ((uint32_t)k[11]);

        mix(a, b, c);
        length -= 12;
        k += 12;
    }

    switch (length) {
        case 12: c += k[11];
        case 11: c += ((uint32_t)k[10]) << 8;
        case 10: c += ((uint32_t)k[9]) << 16;
        case 9 : c += ((uint32_t)k[8]) << 24;
        case 8 : b += k[7];
        case 7 : b += ((uint32_t)k[6]) << 8;
        case 6 : b += ((uint32_t)k[5]) << 16;
        case 5 : b += ((uint32_t)k[4]) << 24;
        case 4 : a += k[3];
        case 3 : a += ((uint32_t)k[2]) << 8;
        case 2 : a += ((uint32_t)k[1]) << 16;
        case 1 : a += ((uint32_t)k[0]) << 24;
            break;
        case 0: return c;
    }

    final(a, b, c);

    return c;
}
