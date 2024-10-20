#ifndef ACESO_KV_UTILS_H
#define ACESO_KV_UTILS_H

#include <infiniband/verbs.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <queue>
#include <deque>
#include <vector>

#include "Common.h"

#define __OUT
#define DDCKV_MAX_SERVER 64

#define SERVER_ID_BMASK     0x3F
#define BLOCK_ADDR_BMASK    0x1FFFFFULL
#define BLOCK_OFF_BMASK     0x3FFFFULL
#define SUBBLOCK_NUM_BMASK  0xF
#define MAX_REP_NUM         10
#define MAX_MEM_NUM         10

#define NO_INVERT_MATRIX -2
#define MMAX 32
#define KMAX 32

enum Role {
    CLIENT,
    SERVER,
    RERROR,
};

enum ClientAllocType {
    TYPE_SUBTABLE    = 1,
    TYPE_KVBLOCK     = 2,
    TYPE_DELTA_KVBLOCK = 3,
};

enum KVLogOp {
    KV_OP_INSERT = 1,
    KV_OP_UPDATE,
    KV_OP_DELETE,
    KV_OP_SEARCH,
    KV_OP_FINISH
};

union U32Pair {
    struct {
        uint32_t first;
        uint32_t second;
    };
    uint64_t combined;
};

struct GlobalConfig {
    uint32_t server_id;
};

struct GlobalInfo {
    int local_id;
    int num_clients;
    int num_memories;

    struct ibv_context * ctx;
    int port_index;
    int device_id;
    int dev_port_id;
    int numa_node_id;

    struct ibv_pd * pd;
    struct ibv_qp * ud_qp;

    int role;
    pthread_mutex_t lock;
};

enum KVMsgType {
    REQ_CONNECT,
    REQ_ALLOC,
    REQ_ALLOC_SUBTABLE,
    REQ_ALLOC_DELTA,
    REQ_SEAL,
    REQ_XOR,
    REQ_CKPT_SEND,
    REQ_CKPT_RECV,
    REQ_CKPT_ENABLE,
    REQ_NULL,

    REP_CONNECT,
    REP_ALLOC,
    REP_ALLOC_SUBTABLE,
    REP_ALLOC_DELTA,
    REP_SEAL,
    REP_XOR,
    REP_CKPT_SEND,
    REP_CKPT_RECV,
    REP_CKPT_ENABLE,
    REP_NULL
};

struct QpInfo {
    uint32_t qp_num;
    uint16_t lid;
    uint8_t  port_num;
    uint8_t  gid[16];
    uint8_t  gid_idx;
};

struct MrInfo {
    uint64_t addr;
    uint32_t rkey;
};

struct IbInfo {
    uint8_t conn_type;
    struct ibv_context   * ib_ctx;
    struct ibv_pd        * ib_pd;
    struct ibv_cq        * ib_cq;
    struct ibv_port_attr * ib_port_attr;
    union  ibv_gid       * ib_gid;
};

struct ConnInfo {
    struct QpInfo qp_info;
    struct MrInfo gc_info;
};

struct TwoMrInfo {
    uint64_t prim_addr;
    uint32_t prim_rkey;
    
    uint64_t pari_addr;
    uint32_t pari_rkey;
};

struct BucketInfo {
    uint64_t addr;
    uint32_t prefix;
};

struct CkptInfo {
    uint64_t new_time;
};

struct XorInfo {
    uint64_t delta_addr;
    uint64_t parity_addr;
    uint64_t xor_size;
    uint64_t index_ver;
};

struct KVMsg {
    uint8_t  type;
    uint8_t  nd_id; // server_id
    uint8_t  th_id; // thread_id
    uint8_t  co_id; // coro_id
    union {
        struct ConnInfo     conn_info;
        struct MrInfo       mr_info;
        struct XorInfo      xor_info;
        struct BucketInfo   bucket_info;
        struct TwoMrInfo    two_mr_info;
        struct CkptInfo     ckpt_info;
    } body;

    KVMsg() : type(0), nd_id(0), th_id(0), co_id(0), body{} {}
};

enum MMBlockRole {
    FREE_BLOCK,
    DATA_BLOCK,
    PARITY_BLOCK,
    DELTA_BLOCK,
};

struct __attribute__((__packed__)) KVLogVersion {
    uint64_t version : 8;   // version is at lower 8-bit
    uint64_t epoch : 56;
};

struct __attribute__((__packed__)) KVLogHeader {
    uint8_t  write_version;    // write version (head)
    uint8_t  op;    
    uint16_t key_length;
    uint32_t value_length;

    union {
        KVLogVersion log;
        uint64_t val;
    } version;

    uint64_t slot_sid : 16;
    uint64_t slot_offset : 48;
};

struct KVLogTail {
    uint8_t  write_version;    // write version (tail)
};

typedef struct TagClientLogMetaInfo {
    uint8_t  pr_server_id;
    uint64_t pr_log_head;
    uint64_t pr_log_tail;
} ClientLogMetaInfo;

typedef struct TagEncodedClientGCSlot {
    uint64_t meta_gc_addr;
} EncodedClientGCSlot;

typedef struct TagClientMetaAddrInfo {
    uint8_t  meta_info_type;
    uint8_t  server_id;
    uint64_t remote_addr;
} ClientMetaAddrInfo;

typedef struct TagDecodedClientGCSlot {
    uint64_t pr_addr;
    uint64_t bk_addr;
    uint8_t  num_subblocks;
} DecodedClientGCSlot;

typedef struct TagLogEntry {
    uint64_t log_id;
    uint64_t hash_value;
    uint64_t old_addr;
    uint64_t new_addr;
} LogEntry;

static inline uint64_t roundup_256(uint64_t len) {
    if (len % 256 == 0) {
        return len;
    }
    return (len / 256 + 1) * 256;
}

static inline uint64_t ConvertAddrToOffset(uint64_t remote_addr) {
    return remote_addr - define::baseAddr;
}

static inline uint64_t ConvertOffsetToAddr(uint64_t offset) {
    return offset + define::baseAddr;
}

static inline bool header_is_valid(KVLogHeader * head) {
    bool is_valid = true;
    uint64_t slot_addr = ConvertOffsetToAddr(head->slot_offset);
    if (head->write_version == 0) { // TODO: check tail write_version
        is_valid = false;
    }
    else if (head->key_length == 0 || head->value_length == 0) {
        is_valid = false;
    }
    else if (slot_addr <= define::baseAddr || slot_addr > (define::baseAddr + HASH_AREA_BORDER)) {
        printf("should not happen\n");
        is_valid = false;
    } 
    else if (head->slot_sid >= define::memoryNodeNum) {
        printf("should not happen\n");
        is_valid = false;
    }
    else if (head->version.val == define::invalidVersion) {
        is_valid = false;
    }
    
    return is_valid;
}

static inline uint64_t time_spent_us(struct timeval * st, struct timeval * et) {
    return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

static inline uint64_t time_spent_ms(struct timeval * st, struct timeval * et) {
    return (et->tv_sec - st->tv_sec) * 1000 + (et->tv_usec - st->tv_usec) / 1000;
}

static inline uint64_t round_up(uint64_t addr, uint64_t align) {
    return ((addr) + align - 1) - ((addr + align - 1) % align);
}

static inline uint64_t round_down(uint64_t addr, uint64_t align) {
    return addr - (addr % align);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg);
void deserialize_kvmsg(__OUT struct KVMsg * kvmsg);
void serialize_qp_info(__OUT struct QpInfo * qp_info);
void deserialize_qp_info(__OUT struct QpInfo * qp_info);
void serialize_mr_info(__OUT struct MrInfo * mr_info);
void deserialize_mr_info(__OUT struct MrInfo * mr_info);
void serialize_conn_info(__OUT struct ConnInfo * conn_info);
void deserialize_conn_info(__OUT struct ConnInfo * conn_info);

int load_config(const char * fname, __OUT struct GlobalConfig * config);

void encode_gc_slot(DecodedClientGCSlot * d_gc_slot, __OUT uint64_t * e_gc_slot);
void decode_gc_slot(uint64_t e_gc_slot, __OUT DecodedClientGCSlot * d_gc_slot);

int stick_this_thread_to_core(int core_id);

uint64_t current_time_us();

void dump_lat_file(char * fname, const std::vector<uint64_t> & lat_vec);

std::string get_load_path(std::string &workload_name);
std::string get_trans_path(std::string &workload_name, uint32_t seq_id);

uint32_t get_unique_id(uint32_t server_id, uint32_t thread_id);

void xor_add_buffers(uint64_t delta_addr, uint64_t parity_addr, size_t length);
void avx_memcpy_128(uint64_t dest, uint64_t src, uint64_t xor_dest, uint64_t xor_src, size_t size);
void avx_memcpy_128_ckpt(uint64_t index_addr, uint64_t ckpt_addr, uint64_t xor_dest, size_t size);
void avx_memcpy_256(uint64_t dest, uint64_t src, uint64_t xor_dest, uint64_t xor_src, size_t size);
void avx_xor_buffers(uint64_t addr1, uint64_t addr2, uint64_t addr3, size_t length);

static inline uint64_t get_meta_addr_degrade(uint64_t block_addr, uint32_t server_id, uint64_t degrade_buf, uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - HASH_AREA_BORDER)  / block_size;
    uint64_t local_block_id =  (block_addr - define::baseAddr - HASH_AREA_BORDER) / block_size;

    uint64_t global_block_id = server_id * block_cnt_per_node + local_block_id;
    return global_block_id * sizeof(MMBlockMeta) + degrade_buf;
}

static inline uint64_t get_meta_addr(uint64_t block_addr, uint32_t server_id, uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - HASH_AREA_BORDER)  / block_size;
    uint64_t local_block_id =  (block_addr - define::baseAddr - HASH_AREA_BORDER) / block_size;

    uint64_t global_block_id = server_id * block_cnt_per_node + local_block_id;
    return global_block_id * sizeof(MMBlockMeta) + define::baseAddr;
}

static inline uint64_t get_meta_addr_with_bid(uint64_t block_id, uint32_t server_id, uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - HASH_AREA_BORDER)  / block_size;
    uint64_t global_block_id = server_id * block_cnt_per_node + block_id;

    return global_block_id * sizeof(MMBlockMeta) + define::baseAddr;
}

static inline uint64_t get_block_addr_with_bid(uint64_t block_id, uint32_t block_size) {
    return block_id * block_size + HASH_AREA_BORDER + define::baseAddr;
}

static inline uint64_t get_total_meta_area_size(uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - HASH_AREA_BORDER)  / block_size;
    return block_cnt_per_node * sizeof(MMBlockMeta) * define::memoryNodeNum;
}

static inline uint64_t get_parity_meta_area_size_per_node(uint64_t client_kv_pari_off, uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - client_kv_pari_off)  / block_size;
    return block_cnt_per_node * sizeof(MMBlockMeta);
}

static inline uint64_t get_parity_meta_area_addr(uint32_t server_id, uint64_t client_kv_pari_off, uint32_t block_size) {
    uint64_t block_cnt_per_node = (define::serverMemorySize - HASH_AREA_BORDER)  / block_size;
    uint64_t normal_cnt_per_node = (client_kv_pari_off - HASH_AREA_BORDER)  / block_size;
    return block_cnt_per_node * sizeof(MMBlockMeta) * server_id + normal_cnt_per_node * sizeof(MMBlockMeta) + define::baseAddr;
}

static inline uint64_t get_normal_block_count_per_node(uint64_t client_kv_pari_off, uint32_t block_size) {
    uint64_t block_cnt_per_node = (client_kv_pari_off - HASH_AREA_BORDER)  / block_size;
    return block_cnt_per_node;
}

static inline uint64_t get_block_addr_with_kv_addr(uint64_t kv_addr, uint32_t block_size) {
    return kv_addr - ((kv_addr - define::baseAddr - HASH_AREA_BORDER) % block_size);
}

bool is_micro_test(std::string & workload_name);
double get_run_time(std::string & workload_name);
std::string micro_get_op_type(std::string & workload_name);

int gf_gen_decode_matrix(unsigned char *encode_matrix,
				unsigned char *decode_matrix,
				unsigned char *invert_matrix,
				unsigned int *decode_index,
				unsigned char *src_err_list,
				unsigned char *src_in_err,
				int nerrs, int nsrcerrs, int k, int m);

#endif