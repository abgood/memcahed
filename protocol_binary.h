#ifndef MEMCACHED_PROTOCOL_BINARY_H
#define MEMCACHED_PROTOCOL_BINARY_H

typedef enum {
    PROTOCOL_BINARY_REQ = 0x80,
    PROTOCOL_BINARY_RES = 0x81
} protocol_binary_magic;

#endif
