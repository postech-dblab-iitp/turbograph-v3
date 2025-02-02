//#define USE_MPK
#ifndef CACHE_CONFIG_H
#define CACHE_CONFIG_H

#define LIGHTNING_MMAP_ADDR 0x30007fff8000

inline uint64_t hash_object_id(uint64_t object_id) {
    // Extracting parts of the object_id and applying modulus operations
    uint64_t first_part = (object_id >> 48) & 0xFFFF; // Extracts the first 16 bits
    uint64_t second_part = (object_id >> 32) & 0xFFFF; // Extracts the next 16 bits
    uint64_t third_part = object_id & 0xFFFFFFFF; // Extracts the last 32 bits

    // Applying modulus operations
    first_part %= 20;
    second_part %= 971;
    third_part %= 52;

    return first_part * 100000 + second_part * 100 + third_part;
}

#endif