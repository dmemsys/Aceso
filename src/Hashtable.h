#ifndef ACESO_HASH_TABLE_H
#define ACESO_HASH_TABLE_H

#include <stdint.h>
#include "KvUtils.h"
#include "Common.h"
#include "city.h"

#define RACE_HASH_INIT_LOCAL_DEPTH          (RACE_HASH_GLOBAL_DEPTH)
#define RACE_HASH_MAX_GLOBAL_DEPTH          (RACE_HASH_GLOBAL_DEPTH)
#define RACE_HASH_SUBTABLE_NUM              (1 << RACE_HASH_GLOBAL_DEPTH)
#define RACE_HASH_INIT_SUBTABLE_NUM         (1 << RACE_HASH_INIT_LOCAL_DEPTH)
#define RACE_HASH_MAX_SUBTABLE_NUM          (1 << RACE_HASH_MAX_GLOBAL_DEPTH)
#define RACE_HASH_ADDRESSABLE_BUCKET_NUM    (34000ULL)
#define RACE_HASH_SUBTABLE_BUCKET_NUM       (RACE_HASH_ADDRESSABLE_BUCKET_NUM * 3 / 2)
#define RACE_HASH_ASSOC_NUM                 (7)
#define SUBTABLE_USED_HASH_BIT_NUM          (32)
#define RACE_HASH_MASK(n)                   ((1 << n) - 1)

#define ROOT_RES_LEN          (sizeof(RaceHashRoot))
#define SUBTABLE_LEN          (RACE_HASH_SUBTABLE_BUCKET_NUM * sizeof(RaceHashBucket))
#define SUBTABLE_RES_LEN      (RACE_HASH_MAX_SUBTABLE_NUM * SUBTABLE_LEN)

struct __attribute__((__packed__)) RaceHashSlot {
    struct __attribute__((__packed__)) SlotAtomic {
        uint64_t fp : 8;
        uint64_t version : 8;
        uint64_t server_id : 8;
        uint64_t offset : 40;
    } atomic;
    
    struct __attribute__((__packed__)) SlotMeta {
        uint64_t epoch : 56;
        uint64_t kv_len : 8;
    } slot_meta;
}; 

struct __attribute__((__packed__)) RaceHashBucket {
    uint32_t local_depth;
    uint32_t prefix;

    uint8_t  unused_bits[8];

    RaceHashSlot slots[RACE_HASH_ASSOC_NUM];
};

typedef struct TagRaceHashSubtableEntry {
    uint8_t lock;
    uint8_t local_depth;
    uint8_t server_id;
    uint8_t pointer[5];
} RaceHashSubtableEntry;

typedef struct TagRaceHashRoot {
    uint64_t index_ver;
    
    uint64_t global_depth;
    uint64_t init_local_depth;
    uint64_t max_global_depth;
    uint64_t prefix_num;
    uint64_t subtable_res_num;
    uint64_t subtable_init_num;
    uint64_t subtable_hash_num;
    uint64_t subtable_hash_range;   // 34000ULL
    uint64_t subtable_bucket_num;
    uint64_t seed;

    uint64_t mem_id;
    uint64_t root_offset;
    uint64_t subtable_offset;
    uint64_t kv_offset;
    uint64_t kv_len;
    
    uint64_t lock;
    RaceHashSubtableEntry subtable_entry[RACE_HASH_MAX_SUBTABLE_NUM][define::replicaIndexNum];
} RaceHashRoot;

struct __attribute__((__packed__)) RaceHashAtomic {
    uint64_t fp : 8;
    uint64_t version : 8;
    uint64_t server_id : 8;
    uint64_t offset : 40;
};

typedef struct TagRaceHashSearchContext {
    int32_t  result;
    int32_t  no_back;
    uint64_t hash_value;
    uint8_t  fp; // fingerprint

    uint64_t f_com_bucket_addr;
    uint64_t s_com_bucket_addr;
    uint64_t read_kv_addr;

    uint64_t f_remote_com_bucket_offset;
    uint64_t s_remote_com_bucket_offset;

    uint64_t read_kv_offset;
    uint32_t read_kv_len;

    RaceHashRoot * local_root;

    void * key;
    uint32_t key_len;
    uint32_t value_len;

    bool sync_root_done;
    bool is_resizing;
} RaceHashSearchContext;

typedef struct TagKVTableAddrInfo {
    uint8_t     server_id[define::replicaIndexNum];
    uint64_t    f_bucket_addr[define::replicaIndexNum];
    uint64_t    s_bucket_addr[define::replicaIndexNum];
    uint32_t    f_main_idx;
    uint32_t    s_main_idx;
    uint32_t    f_idx;
    uint32_t    s_idx;
} KVTableAddrInfo;

typedef struct TagKVHashInfo {
    uint64_t hash_value;
    uint64_t prefix;
    uint8_t  fp;
    uint8_t  local_depth;
} KVHashInfo;

typedef struct TagKVInfo {
    void   * l_addr;
    uint32_t lkey;
    uint32_t key_len;
    uint32_t value_len;
} KVInfo;

typedef struct TagKVRWAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;
    uint64_t l_kv_addr;
    uint32_t length;
} KVRWAddr;

typedef struct TagKVCASAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;
    uint64_t l_kv_addr;
    uint64_t orig_value;
    uint64_t swap_value;
} KVCASAddr;

typedef struct TagKVXORAddr {
    uint8_t  server_id;
    uint64_t r_kv_addr;     // slot remote addr
    uint64_t l_kv_addr;     // for fetched data
    uint32_t rkey;
    uint32_t lkey;
    uint64_t add_value;    // for xor data
} KVXORAddr;

struct KVRecoverAddr {
    bool need_xor;  // if need_xor = false, then only cares l_kv_addr and r_kv_addr
    bool local_hit; // for server use only
    uint64_t l_kv_addr;
    uint64_t r_kv_addr;
    uint32_t server_id;

    uint32_t remote_server_id[16];
    uint64_t remote_addr[16];

    uint64_t local_addr[16];
    uint32_t local_addr_count;

    std::vector<uint32_t> prim_idxs; 
    std::vector<uint32_t> delt_idxs; 
};

struct AddrCacheEntry {
    std::atomic<bool>                       read_kv;
    std::atomic<uint32_t>                   server_id;
    std::atomic<uint64_t>                   r_slot_addr;
    std::atomic<RaceHashSlot::SlotAtomic>   l_slot_atomic;
    std::atomic<RaceHashSlot::SlotMeta>     l_slot_meta;
    std::atomic<uint32_t>                   miss_cnt;
    std::atomic<uint32_t>                   acc_cnt;

    void store_slot(RaceHashSlot * new_slot) {
        l_slot_atomic.store(new_slot->atomic);
        l_slot_meta.store(new_slot->slot_meta);
    }

    RaceHashSlot load_slot() {
        RaceHashSlot cache_slot;
        cache_slot.atomic = l_slot_atomic.load();
        cache_slot.slot_meta = l_slot_meta.load();
        return cache_slot;
    }
};

static inline uint64_t SubtableFirstIndex(uint64_t hash_value, uint64_t capacity) {
    return hash_value % (capacity / 2);
}

static inline uint64_t SubtableSecondIndex(uint64_t hash_value, uint64_t f_index, uint64_t capacity) {
    uint32_t hash = hash_value;
    uint16_t partial = (uint16_t)(hash >> 16);
    uint16_t non_sero_tag = (partial >> 1 << 1) + 1;
    uint64_t hash_of_tag = (uint64_t)(non_sero_tag * 0xc6a4a7935bd1e995);
    return (uint64_t)(((uint64_t)(f_index) ^ hash_of_tag) % (capacity / 2) + capacity / 2);
}

static inline uint64_t HashIndexConvert40To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32)
        | ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) 
        | ((uint64_t)addr[4] << 8);
}

static inline void HashIndexConvert64To40Bits(uint64_t addr, __OUT uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 8)  & 0xFF);
}

static inline bool IsEmptySlot(RaceHashSlot * slot) {
    if (slot->atomic.fp == 0 && slot->atomic.offset == 0 && slot->atomic.server_id == 0) {
        return true;
    } else {
        return false;
    }
}

static inline bool IsEmptySlotInsert(RaceHashSlot * slot) {
    if (slot->atomic.fp == 0 && slot->atomic.offset == 0 && slot->atomic.server_id == 0 && slot->atomic.version == 0 && slot->slot_meta.epoch == 0) {
        return true;
    } else {
        return false;
    }
}

static inline bool IsEqualSlot(RaceHashSlot * new_slot, RaceHashSlot * old_slot) {
    uint64_t new_atomic_val = *(uint64_t *)&new_slot->atomic;
    uint64_t old_atomic_val = *(uint64_t *)&old_slot->atomic;

    if (new_atomic_val == old_atomic_val)
        return true;
    else
        return false;
}

static inline uint64_t ConvertSlotToUInt64(RaceHashSlot * slot) {
    return *(uint64_t *)&slot->atomic;
}

static inline uint64_t ConvertSlotToAddr(RaceHashSlot * slot) {
    return slot->atomic.offset + define::baseAddr;
}

uint32_t HashIndexComputeServerId(uint64_t hash, uint32_t num_memory);
uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed);
uint8_t  HashIndexComputeFp(uint64_t hash);
uint8_t KVAddrComputeFp(uint64_t addr);
uint32_t GetFreeSlotNum(RaceHashBucket * bucekt, __OUT uint32_t * free_idx);
bool IsEmptyPointer(uint8_t * pointer, uint32_t num);
bool CheckKey(void * r_key_addr, uint32_t r_key_len, void * l_key_addr, uint32_t l_key_len);

#endif