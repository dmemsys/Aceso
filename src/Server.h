#ifndef ACESO_SERVER_H
#define ACESO_SERVER_H
#include <unistd.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <assert.h>
#include <errno.h>

#include <map>
#include <unordered_map>
#include <vector>
#include <queue>

#include <boost/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <condition_variable>
#include <time.h>
#include <chrono>

#include <infiniband/verbs.h>
#include <immintrin.h>
#include <omp.h>
#include <libmemcached/memcached.h>
#include "isa-l.h"
#include "lz4.h"

#include "NetworkManager.h"
#include "KvUtils.h"
#include "Hashtable.h"
#include "Rdma.h"

#define MAP_HUGE_2MB        (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB        (30 << MAP_HUGE_SHIFT)
#define RECOVER_BUF_LEN     (2ULL * 1024 * 1024 * 1024)

struct XorTask {
    struct KVMsg request;
};

struct MMBlockRecoverContext {
    bool is_remote;
    uint64_t primary_addr;

    uint32_t remote_server_id[16];
    uint64_t remote_addr[16];

    uint64_t local_addr[16];
    uint32_t local_addr_count;

    std::vector<uint32_t> prim_idxs; 
    std::vector<uint32_t> delt_idxs; 
};

class Server {
public:
    uint32_t my_server_id;
    volatile uint8_t need_stop;
    bool is_recovery;
    
    RdmaContext rdma_ctx;
    NetworkManager * network_manager;

    volatile uint64_t index_ver;
    uint8_t crash_happen;
    uint32_t ckpt_interval;
    volatile bool ckpt_start;
    uint64_t hash_ckpt_addr[3];     // RDMA memory, storing index and checkpoints
    uint64_t hash_delta_addr[3];    // RDMA memory, storing index delta
    uint32_t hash_ckpt_idx;
    uint32_t hash_ckpt_send; 
    uint32_t hash_ckpt_recv; 
    void * hash_local_buf[3];       // local buffer, storing temporary data, which can be smaller by dividing the index into multiple smaller chunks, checkpointing one chunk at a time.
    

private:
    uint64_t client_meta_area_off;  // num_parity_blocks * struct MMBlockMeta
    uint64_t client_meta_area_len;
    uint64_t client_hash_area_off;
    uint64_t client_hash_area_len;
    uint64_t client_kv_area_off;
    uint64_t client_kv_pari_off;
    uint64_t client_kv_area_len;

    uint32_t num_blocks;
    uint32_t num_parity_blocks;
    uint32_t slab_num;
    uint64_t slab_size;

    uint32_t block_size;
    uint32_t subblock_num;
    uint32_t bmap_subblock_num;
    uint32_t data_subblock_num;
    uint32_t alloc_watermark;

    std::vector<bool> subtable_alloc_map;
    std::queue<uint64_t> allocable_blocks;
    std::unordered_map<uint64_t, bool> allocated_blocks;
    
    void * data_raw;
    struct ibv_mr * ib_mr;
    
    // for recovery
    void * recover_buf; // currently point to the parity area
    uint64_t recover_buf_ptr;
    std::unordered_map<uint64_t, std::vector<uint64_t> > parity_slab_map;
    std::unordered_map<uint64_t, std::vector<uint64_t> > primar_slab_map;
    std::vector<MMBlockRecoverContext> remote_block_list;
    std::vector<MMBlockRecoverContext> local_block_list;
    uint64_t index_ver_before_crash;
    uint64_t total_check_count;
    std::unordered_map<uint64_t, uint64_t> local_block_map;
    RaceHashRoot * race_root;
    uint64_t kv_check_buf;
    uint64_t check_valid_kv_cnt;
    uint64_t check_lost_r_kv_cnt;
    uint64_t check_lost_l_kv_cnt;


    std::mutex send_pool_lock;

    // for RS encoding
	uint8_t * encode_matrix, * encode_table;
    // for RS decoding
    uint32_t  decode_index[3][32];
    uint8_t * decode_matrix[3], * invert_matrix[3], * decode_table[3];
	uint8_t   src_in_err[3][32], src_err_list[3][32];
	uint8_t * recov[3][32];
    uint8_t * temp_buff_list[3];

private:
    // server memory
    int _init_root(void * root_addr);
    int _init_subtable(void * subtable_addr);
    int _init_hashtable();
    void _init_layout();
    void _init_slab_map();
    void _init_block();
    void _init_memory();
    void _init_ckpt();
    void _init_rs_code();

    uint64_t _mm_alloc();
    uint64_t _mm_alloc_subtable();
    int _mm_free(uint64_t st_addr);

    uint32_t _get_rkey();
    int _get_mr_info(__OUT struct MrInfo * mr_info);
    uint64_t _get_kv_area_addr();
    uint64_t _get_subtable_st_addr();
    uint32_t _get_index_in_xor_map(uint64_t primar_addr);

    // server
    struct KVMsg * _prepare_reply(const struct KVMsg * request);
    struct KVMsg * _prepare_request();

    // ckpt
    int _ckpt_check_prev();
    int _ckpt_set_index_ver(uint64_t index_ver);
    int _ckpt_send_index_simple(const struct KVMsg * request);
    int _ckpt_send_index(const struct KVMsg * request);
    int _ckpt_recv_index(const struct KVMsg * request);
    void _fill_write_index_ror(RdmaOpRegion * ror, uint64_t local_addr, uint64_t write_size);
    void _fill_write_index_ckpt_ror(RdmaOpRegion * ror, uint64_t local_addr, uint64_t write_size);

    // recovery util functions
    uint64_t _get_new_recover_block_addr();
    void _convert_addr_to_offset(uint64_t addr, uint64_t *offset, uint64_t *local_offset);
    void _convert_offset_to_addr(uint64_t *addr, uint64_t offset, uint64_t local_offset);
    void _get_parity_slab_offsets(uint64_t offset, uint32_t server_id, uint64_t parity_offsets[], uint32_t parity_server_ids[]);
    void _get_primary_slab_offsets(uint64_t offset, uint32_t server_id, std::vector<uint32_t> & primary_server_ids, std::vector<uint64_t> & primary_offsets);
    void _prepare_new_recover_context(MMBlockRecoverContext * ctx);
    void _xor_all_block(std::vector<MMBlockRecoverContext> & recover_list);
    void _xor_one_block(MMBlockRecoverContext * ctx);
    void _read_all_block(std::vector<MMBlockRecoverContext> & recover_list);
    void _rs_update(uint64_t delta_addr, uint64_t parity_addr, size_t length, uint32_t data_idx); // for RS coding test

    // recovery sub functions
    int _recover_meta_state();
    int _recover_old_index();
    int _recover_subtable_state();
    int _recover_local_new_block();
    int _recover_remote_new_block();
    int _recover_check_all();
    int _recover_check_one_block(MMBlockRecoverContext & ctx);
    int _recover_check_one_kv(uint64_t l_kv_addr, uint64_t r_kv_addr, uint32_t remote_sid);
    inline RaceHashSlot * _recover_locate_slot(uint64_t l_kv_addr, uint64_t r_kv_addr, uint32_t remote_sid, uint64_t hash_value, uint64_t hash_prefix, uint8_t hash_fp);
    void _recover_prepare_kv_recover(KVRecoverAddr & ctx, uint64_t & l_kv_buf_addr);
    void _fill_recover_kv_ror(RdmaOpRegion * ror, const std::vector<KVRecoverAddr> & r_addr_list);
    void _xor_all_kv(std::vector<KVRecoverAddr> & recover_list);
    void _xor_one_kv(KVRecoverAddr & ctx);
    
    // recovery main functions
    int _recover_server();
    int _recover_metadata();
    int _recover_index();
    int _recover_block();
    

public:
    Server(uint8_t server_id, bool is_recovery = false, uint32_t ckpt_interval = define::ckptInterval, uint32_t block_size = define::memoryBlockSize);
    ~Server();

    int server_on_connect(const struct KVMsg * request);
    int server_on_alloc(const struct KVMsg * request);
    int server_on_alloc_subtable(const struct KVMsg * request);
    int server_on_alloc_delta(const struct KVMsg * request);
    int server_on_seal(const struct KVMsg * request);
    int server_on_xor(const struct KVMsg * request);
    int server_on_ckpt_send(const struct KVMsg * request);
    int server_on_ckpt_recv(const struct KVMsg * request);
    int server_on_ckpt_enable(const struct KVMsg * request);
    int server_ckpt_iteration_start();
    int server_ckpt_iteration_wait();

    void * thread_main();

    void stop();
};

typedef struct TagServerMainArgs {
    Server * server;
} ServerMainArgs;

void * server_main(void * server_main_args);

#endif