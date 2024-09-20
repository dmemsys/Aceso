#include <iostream>
#include <cstring>
#include <chrono>
#include <stdio.h> 
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>
#include <assert.h>
#define MAP_HUGE_2MB  (21 << MAP_HUGE_SHIFT)

const size_t TOTAL_SIZE = 2ULL * 1024 * 1024 * 1024; // 2GB
const size_t COPY_SIZE = 2 * 1024 * 1024; // 2MB

static inline uint64_t round_up(uint64_t addr, uint64_t align) {
    return ((addr) + align - 1) - ((addr + align - 1) % align);
}

int main() {
    void * buffer = NULL;
    int port_flag = PROT_READ | PROT_WRITE;
    int mm_flag   = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
    buffer = mmap(NULL, (size_t)TOTAL_SIZE, port_flag, mm_flag, -1, 0);
    if (buffer == MAP_FAILED) {
        printf("not enough huge-page memory\n");
        return 0;
    }
    char * dest = (char *)buffer;

    std::memset(buffer, 'A', TOTAL_SIZE);

    size_t randomOffset = round_up(rand() % (TOTAL_SIZE - 2 * COPY_SIZE) + COPY_SIZE, COPY_SIZE) ;

    auto start = std::chrono::high_resolution_clock::now();
    std::memcpy(dest, dest + randomOffset, COPY_SIZE);
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::micro> duration = end - start;
    double latency = duration.count();
    double throughput = COPY_SIZE / duration.count() * 1000 * 1000 / 1024 / 1024;

    std::cout << "Average Latency: " << latency << " microseconds" << std::endl;
    std::cout << "Throughput: " << throughput << " MB/s" << std::endl;

    return 0;
}