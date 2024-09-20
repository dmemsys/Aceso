#include "AddrCache.h"
#include "Common.h"
#include "Rdma.h"
#include "KvUtils.h"

#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <atomic>
#include <mutex>

void AddrCache::cache_update(std::string &key_str, uint32_t server_id, RaceHashSlot * l_slot_ptr, uint64_t r_slot_addr) {
    AddrCacheEntry * old_entry = (AddrCacheEntry *)cache_map[key_str];
    
    if (old_entry) {
        RaceHashSlot old_slot_val = old_entry->load_slot();
        if (!IsEqualSlot(l_slot_ptr, &old_slot_val)) {
            old_entry->store_slot(l_slot_ptr);
            old_entry->r_slot_addr.store(r_slot_addr);
            old_entry->server_id.store(server_id);
        }
        old_entry->acc_cnt++;
        return;
    }
    
    AddrCacheEntry * new_entry = (AddrCacheEntry *)malloc(sizeof(AddrCacheEntry));
    new_entry->store_slot(l_slot_ptr);
    new_entry->r_slot_addr = r_slot_addr;
    new_entry->server_id = server_id;
    new_entry->acc_cnt  = 1;
    new_entry->miss_cnt = 0;

    if (__sync_bool_compare_and_swap(&(cache_map[key_str]), old_entry, new_entry)) {
        _safely_delete(old_entry);
    }
    else {
        free(new_entry);
    }
}

AddrCacheEntry * AddrCache::cache_search(std::string &key_str) {
    AddrCacheEntry * old_entry = (AddrCacheEntry *)cache_map[key_str];
    if (old_entry == 0UL) {
        return NULL;
    }

    RaceHashSlot old_slot_val = old_entry->load_slot();
    if (IsEmptySlot(&old_slot_val)) {
        if (__sync_bool_compare_and_swap(&(cache_map[key_str]), old_entry, 0UL)) {
            _safely_delete(old_entry);
        }
        return NULL;
    }
    
    uint32_t miss_cnt = old_entry->miss_cnt.load();
    uint32_t acc_cnt = old_entry->acc_cnt.load();
    float miss_rate = ((float) miss_cnt / acc_cnt);
    if (miss_rate > define::missRateThreash) {
        old_entry->read_kv = false;
    } else {
        old_entry->read_kv = true;
    }

    return old_entry;
}

void AddrCache::cache_delete(std::string &key_str) {
    AddrCacheEntry * old_entry = (AddrCacheEntry *)cache_map[key_str];
    if (old_entry == 0UL) {
        return;
    }
    if (__sync_bool_compare_and_swap(&(cache_map[key_str]), old_entry, 0UL)) {
        _safely_delete(old_entry);
    }
}

void AddrCache::_safely_delete(AddrCacheEntry *cache_entry) {
  cache_entry_gc.push(cache_entry);
  while (cache_entry_gc.unsafe_size() > safely_free_epoch) {
    AddrCacheEntry* next;
    if (cache_entry_gc.try_pop(next)) {
      free(next);
    }
  }
}

void AddrCache::cache_reinit() {
    cache_map.clear();
}