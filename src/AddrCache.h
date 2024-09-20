#ifndef ACESO_MIX_CACHE_H
#define ACESO_MIX_CACHE_H

#include "Common.h"
#include "Rdma.h"
#include "NetworkManager.h"
#include "Hashtable.h"

#include "city.h"
#include <infiniband/verbs.h>
#include <queue>
#include <set>
#include <atomic>
#include <mutex>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>

class AddrCache {
private:
    tbb::concurrent_unordered_map<std::string, AddrCacheEntry *> cache_map;
    tbb::concurrent_queue<AddrCacheEntry*> cache_entry_gc;
    static const int safely_free_epoch = 1024;

private:
    void _safely_delete(AddrCacheEntry *cache_entry);

public:
    AddrCache() {}

    void cache_update(std::string &key_str, uint32_t server_id, RaceHashSlot * l_slot_ptr, uint64_t r_slot_addr);
    AddrCacheEntry * cache_search(std::string &key_str);
    void cache_delete(std::string &key_str);
    void cache_reinit();
};
#endif