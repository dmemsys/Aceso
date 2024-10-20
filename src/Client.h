#ifndef ACESO_CLIENT_H
#define ACESO_CLIENT_H

#include <map>

#include <pthread.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <unordered_map>
#include <boost/fiber/all.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/policies.hpp>
#include <tbb/concurrent_queue.h>
#include <numa.h>
#include <sched.h>
#include "isa-l.h"

#include "KvUtils.h"
#include "Hashtable.h"
#include "NetworkManager.h"
#include "AddrCache.h"

typedef struct TagSubblockInfo {
    uint64_t remote_addr[define::replicaNum];
    uint32_t server_id[define::replicaNum];
    uint64_t local_mmblock_addr;
} SubblockInfo;

typedef struct TagClientMMAllocCtx {
    uint8_t  server_id[define::replicaNum];
    uint64_t remote_addr[define::replicaNum];

    uint64_t prev_addr[define::replicaNum];
    uint8_t  prev_server_id[define::replicaNum];
    
    uint64_t next_addr[define::replicaNum];
    uint8_t  next_server_id[define::replicaNum];

    uint64_t local_mmblock_addr;
    uint64_t prev_mmblock_addr;
    uint64_t next_mmblock_addr;
} ClientMMAllocCtx;

typedef struct TagClientMMAllocSubtableCtx {
    uint8_t  server_id;
    uint64_t addr;
} ClientMMAllocSubtableCtx;

typedef struct TagClientMMBlock {
    struct MrInfo mr_info;
    uint8_t server_id;

    struct MrInfo parity_mr_info_list[2];
    uint8_t parity_server_id_list[2];

    struct MrInfo delta_mr_info_list[2];

    bool * bmap;
    std::atomic<uint32_t> num_allocated;
    std::atomic<uint32_t> max_allocated;
    std::atomic<uint32_t> inv_allocated;
    int32_t  prev_free_subblock_idx;
    int32_t  next_free_subblock_idx;
    int32_t  next_free_subblock_cnt;
    uint64_t valid_bitmap[256] __attribute__((aligned(16)));
    uint64_t inval_bitmap[256] __attribute__((aligned(16)));
    uint64_t index_ver;
} ClientMMBlock;

struct MMBlockRegInfo {
    struct MrInfo mr_info;
    uint8_t server_id;

    struct MrInfo parity_mr_info_list[2];
    uint8_t parity_server_id_list[2];

    struct MrInfo delta_mr_info_list[2];
};

union ReturnValue{
    void * value_addr;    // for search return value
    int    ret_code;
};

enum KVOpsRetCode {
    KV_OPS_SUCCESS = 0,
    KV_OPS_FAIL_RETURN,
    KV_OPS_FAIL_REDO
};

// each req ctx represents a request and its state
typedef struct TagKVReqCtx {
    // input
    CoroContext    * coro_ctx;
    uint64_t         coro_id;
    uint8_t          req_type;
    KVInfo         * kv_info;

    RaceHashBucket * local_bucket_addr;  // need to be allocated to each coro ib_mr
    void           * local_cache_addr;   // need to be allocated to each coro ib_mr
    void           * local_kv_addr;      // need to be allocated to each coro ib_mr
    void           * local_cas_target_value_addr;     // need to be allocated to each coro ib_mr
    void           * local_cas_return_value_addr;     // need to be allocated to each coro ib_mr
    void           * local_slot_addr;
    uint32_t         lkey;
    bool             use_cache;

    // bucket_info
    RaceHashBucket * bucket_arr[4];
    RaceHashBucket * f_com_bucket;
    RaceHashBucket * s_com_bucket;
    RaceHashSlot   * slot_arr[4]; 

    // for preparation
    KVHashInfo      hash_info;
    KVTableAddrInfo tbl_addr_info;
    
    // for key-value pair read
    std::vector<KVRWAddr>                     kv_read_addr_list;
    std::vector<std::pair<int32_t, int32_t>>  kv_idx_list;
    std::vector<KVRWAddr>                     kv_ver_addr_list;
    std::vector<KVRecoverAddr>                kv_recover_addr_list; // for degraded search

    // for kv insert/update/delete
    ClientMMAllocCtx mm_alloc_ctx;
    KVCASAddr kv_modify_pr_cas_addr[define::replicaIndexNum];

    // for insert
    int32_t bucket_idx; // bucket_idx:0~3，slot_idx:0~6
    int32_t slot_idx;
    int32_t match_idx;

    // for kv update/delete
    KVRWAddr kv_invalid_addr;

    bool is_finished;
    bool has_allocated;
    bool has_finished;
    bool is_sim_gc;                // whether to simulate GC
    bool has_previous;

    ReturnValue ret_val;
    KVLogHeader * read_header;

    volatile bool * should_stop;

    std::string key_str;
    bool is_prev_kv_hit;
    bool is_curr_kv_hit;
    AddrCacheEntry * cache_entry;
    RaceHashSlot cache_slot;
    bool cache_read_kv;

    uint64_t prev_ver;
    RaceHashSlot * new_slot;
} KVReqCtx;

class Client {
public:
    AddrCache  * addr_cache;
    KVInfo     * kv_info_list;
    KVReqCtx   * kv_req_ctx_list;
    uint32_t     req_total_num;
    uint32_t     req_local_num;
    uint32_t     my_unique_id;
    uint32_t     my_coro_num;

    // for perf test
    uint64_t     req_finish_num;
    uint64_t     req_failed_num;
    uint64_t     req_search_num[5];
    uint64_t     req_modify_num;
    uint64_t     req_cas_num;
    uint64_t     cache_hit_num;
    uint64_t     cache_mis_num;
    uint64_t   * req_latency[4];
    uint64_t     cur_valid_kv_sz;
    uint64_t     raw_valid_kv_sz;

private:
    CoroCall worker[define::maxCoroNum + 1];    // +1 for GC
    CoroCall master;
    CoroQueue busy_waiting_queue;
    uint32_t lock_coro_id;
    
    RdmaContext rdma_ctx;
    NetworkManager *network_manager;
    
    uint64_t remote_global_meta_addr;
    uint64_t remote_meta_addr;
    uint64_t remote_root_addr;
    uint32_t my_server_id;
    uint32_t my_thread_id;
    uint32_t num_memory;
    uint32_t last_allocated_mn;
    uint64_t * coro_local_addr_list;

    uint32_t block_size;
    uint32_t subblock_num;
    uint32_t bmap_subblock_num;
    uint32_t data_subblock_num;
    uint32_t alloc_watermark;

    RaceHashRoot * race_root;
    void * local_buf;
    void * input_buf;   // pointer to local_buf
    struct ibv_mr * local_buf_mr;

    std::vector<ClientMMBlock *> mm_blocks;
    std::deque<SubblockInfo> subblock_free_queue;
    SubblockInfo             last_allocated_info;

    uint32_t slab_num;
    uint64_t slab_size;
    std::unordered_map<uint64_t, std::vector<uint64_t> > parity_slab_map;
    std::unordered_map<uint64_t, std::vector<uint64_t> > primar_slab_map;

    // for free
    std::unordered_map<std::string, uint64_t> free_bit_map;
    // for degraded search
    void * degrade_buf;
    void * sim_gc_buf;
    uint32_t crash_server_id;
    // for degraded search - RS encoding
	uint8_t * encode_matrix, * encode_table;
    // for degraded search - RS decoding
    uint32_t  decode_index[3][32];
    uint8_t * decode_matrix[3], * invert_matrix[3], * decode_table[3];
	uint8_t   src_in_err[3][32], src_err_list[3][32];
	uint8_t * recov[3][32];
    uint8_t * temp_buff_list[3];

// private inline methods
private:
    inline uint8_t _get_alloc_mn_id(void) {
        return (last_allocated_mn++) % define::memoryNodeNum;
    }
    inline int _write_race_root() {
        for (int i = 0; i < define::memoryNodeNum; i ++) {
            int ret = network_manager->rdma_write_sync((uint64_t)race_root, remote_root_addr, sizeof(RaceHashRoot), i);
            assert(ret == 0);
        }
        return 0;
    }
    inline int _get_race_root() {
        int ret = network_manager->rdma_read_sync((uint64_t)race_root, remote_root_addr, sizeof(RaceHashRoot), 0);
        assert(ret == 0);
        return 0;
    }

// private methods
private:
    // memory management
    void _init_slab_map(void);
    void _init_rs_code(void);
    void _init_block_for_load(void);
    void _init_mmblock(ClientMMBlock * mm_block, MMBlockRegInfo * reg_info);
    void _convert_addr_to_offset(uint64_t addr, uint64_t *offset, uint64_t *local_offset);
    void _get_parity_slab_offsets(uint64_t offset, uint32_t server_id, uint64_t parity_offsets[], uint32_t parity_server_ids[]);
    void _convert_offset_to_addr(uint64_t *addr, uint64_t offset, uint64_t local_offset);
    void _mm_alloc(KVReqCtx * ctx, size_t size, __OUT ClientMMAllocCtx * mm_ctx);
    void _mm_free_cur(const ClientMMAllocCtx * ctx);
    void _mm_free(uint64_t old_slot);
    void _alloc_memory_block(KVReqCtx * ctx, std::deque<SubblockInfo> &free_queue);
    void _alloc_subtable(KVReqCtx * ctx, __OUT ClientMMAllocSubtableCtx * mm_ctx);
    int _start_ckpt(void);
    int _alloc_from_sid(KVReqCtx * ctx, uint32_t server_id, int alloc_type, __OUT struct MrInfo * mr_info);
    int _alloc_delta_from_sid(KVReqCtx * ctx, uint32_t server_id, int alloc_type, uint32_t primar_server_id, struct MrInfo * primar_mr_info, struct MrInfo * parity_mr_info, __OUT struct MrInfo * delta_mr_info);
    int _dyn_reg_new_subtable(uint32_t server_id, int alloc_type, __OUT struct MrInfo * mr_info);
    int _dyn_reg_new_block(KVReqCtx * ctx, MMBlockRegInfo * reg_info, int alloc_type, std::deque<SubblockInfo> &free_queue);
    int _init_hash_table(void);
    int _write_new_block_meta(CoroContext *ctx, MMBlockRegInfo * reg_info);
    int _write_seal_block_meta(CoroContext *ctx, uint64_t block_addr, uint32_t server_id, uint64_t index_ver);
    int _xor_mem_block(KVReqCtx * ctx, ClientMMBlock * mm_block);
    struct KVMsg * _get_new_request(KVReqCtx * ctx);
    uint32_t _get_index_in_xor_map(uint64_t pr_addr);
    CoroContext * _get_coro_ctx(KVReqCtx * ctx);
    
    void _prepare_request(KVReqCtx * ctx);
    void _cache_search(KVReqCtx * ctx);
    void _get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * kv_hash_info);
    void _get_kv_addr_info(KVHashInfo * kv_hash_info, __OUT KVTableAddrInfo * kv_addr_info);
    void _find_kv_in_buckets(KVReqCtx * ctx);
    void _get_local_bucket_info(KVReqCtx * ctx);
    int32_t _find_match_slot(KVReqCtx * ctx);
    void _find_empty_slot(KVReqCtx * ctx);
    void _alloc_new_kv(KVReqCtx * ctx);
    void _write_new_kv(KVReqCtx * ctx);
    void _prepare_ver_addr(KVReqCtx * ctx);
    void _prepare_old_slot(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot ** old_slot, RaceHashSlot ** new_slot);
    void _req_finish(KVReqCtx * ctx);
    void _fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, RaceHashSlot * old_slot, __OUT RaceHashSlot * new_slot);
    void _fill_cas_addr(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot * old_slot, RaceHashSlot * new_slot);
    void _modify_primary_idx(KVReqCtx * ctx);
    bool _cache_entry_is_valid(KVReqCtx * ctx);
    bool _need_degrade_search(uint64_t remote_addr, uint32_t server_id);
    void _find_kv_in_buckets_degrade(KVReqCtx * ctx);
    void _prepare_kv_recover_addr(KVRecoverAddr & ctx, uint64_t & local_kv_buf_addr);
    void _get_primary_slab_offsets(uint64_t offset, uint32_t server_id, std::vector<uint32_t> & primary_server_ids, std::vector<uint64_t> & primary_offsets);
    void _rs_update(uint64_t delta_addr, uint64_t parity_addr, size_t length, uint32_t data_idx);
    void _xor_one_kv(KVRecoverAddr & ctx);
    void _xor_all_kv(std::vector<KVRecoverAddr> & recover_list);
    int32_t _find_match_kv_idx_degrade(KVReqCtx * ctx);
    void _sim_gc_fetch_block(KVReqCtx * ctx, uint64_t remote_addr, uint32_t server_id);
    void _sim_gc_xor_kv(KVReqCtx * ctx, SubblockInfo & alloc_subblock);
    void _cache_read_prev_kv(KVReqCtx * ctx);
    void _cache_read_curr_kv(KVReqCtx * ctx);

    // kv operation related
    void _fill_read_bucket_ror(RdmaOpRegion * ror, KVReqCtx * ctx);
    void _fill_read_cache_kv_ror(RdmaOpRegion * ror, RaceHashSlot * local_slot_val, uint64_t local_addr);
    void _fill_read_slot_ror(RdmaOpRegion * ror, uint32_t server_id, uint64_t r_slot_addr, uint64_t l_slot_addr);
    void _fill_read_kv_ror(RdmaOpRegion * ror, KVReqCtx * ctx, RaceHashSlot * local_slot);
    void _fill_read_kv_ror(RdmaOpRegion * ror, const std::vector<KVRWAddr> & r_addr_list);
    void _fill_write_kv_ror(RdmaOpRegion * ror, KVInfo * a_kv_info, ClientMMAllocCtx * r_mm_info);
    void _fill_cas_index_ror(RdmaOpRegion * ror, const KVCASAddr * cas_addr);
    void _fill_write_ver_ror(RdmaOpRegion * ror, std::vector<KVRWAddr> & ver_addr_list, void * local_addr, uint32_t length);
    void _fill_read_index_ror(RdmaOpRegion * ror, const KVCASAddr & cas_addr);
    void _fill_recover_kv_ror(RdmaOpRegion * ror, const std::vector<KVRecoverAddr> & r_addr_list);

    void _kv_search_read_cache(KVReqCtx * ctx);
    void _kv_search_read_buckets(KVReqCtx * ctx);
    void _kv_search_read_kv_from_buckets(KVReqCtx * ctx);
    void _kv_search_check_kv_from_buckets(KVReqCtx * ctx);
    void _kv_update_read_cache(KVReqCtx * ctx);
    void _kv_update_read_buckets(KVReqCtx * ctx);
    void _kv_update_read_kv_from_buckets(KVReqCtx * ctx);
    void _kv_update_write_kv_and_prepare_cas(KVReqCtx * ctx);
    void _kv_update_cas_primary(KVReqCtx * ctx);
    // for degraded search
    void _kv_search_read_buckets_degrade(KVReqCtx * ctx);
    void _kv_search_read_kv_degrade(KVReqCtx * ctx);
    void _kv_search_check_kv_degrade(KVReqCtx * ctx);

// public inline methods
public:
    
    // public methods
public:
    Client(uint8_t server_id, uint8_t thread_id, uint32_t client_num, AddrCache * addr_cache, bool is_recovery = false, uint32_t block_size = define::memoryBlockSize);
    ~Client();

    int kv_insert(KVReqCtx * ctx);
    int kv_delete(KVReqCtx * ctx);
    int kv_update(KVReqCtx * ctx);
    int kv_update_gc(KVReqCtx * ctx);
    void * kv_search(KVReqCtx * ctx);
    void * kv_search_degrade(KVReqCtx * ctx);

    using WorkFunc = std::function<ReturnValue (Client *, KVReqCtx *)>;
    void run_coroutine(volatile bool *should_stop, WorkFunc work_func, uint32_t coro_num);
    void coro_worker(CoroYield &yield, int coro_id, volatile bool *should_stop, WorkFunc work_func);
    void coro_gc(CoroYield &yield, int coro_id, volatile bool *should_stop);
    void coro_master(CoroYield &yield, volatile bool *should_stop);

    // for perf test
    int load_kv_requests(uint32_t st_idx, int32_t num_ops, std::string workload_name = "", std::string op_type = "");
    void init_kv_req_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops, CoroContext *coro_ctx);
    void client_barrier(const std::string &str);
    void start_degrade(uint32_t crash_server_id);
};
#endif