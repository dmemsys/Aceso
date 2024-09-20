#ifndef ACESO_NETWORK_MANAGER_H
#define ACESO_NETWORK_MANAGER_H

#include <infiniband/verbs.h>
#include <stdint.h>
#include <netdb.h>

#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <tbb/concurrent_hash_map.h>
#include <libmemcached/memcached.h>

#include "Rdma.h"
#include "KvUtils.h"

// Info published by the UD QPs
struct UdAttr {
	uint32_t lid;
	uint32_t qp_num;
    uint8_t  gid[16];
};

enum RpcType : uint8_t {
    RPC_REQ,
    RPC_ACK,
    RPC_ERROR,
};

struct RpcMessage {
    RpcType type;
    
    uint16_t src_sid;
    uint16_t coro_id;

    uint16_t key_len;
    char key_str[46];
} __attribute__((packed));

class NetworkManager {
private:
    uint32_t udp_sock;
    uint16_t udp_port;
    uint8_t  role;
    uint8_t  conn_type;
    struct sockaddr_in * server_addr_list;
    uint32_t num_memory;
    uint32_t my_server_id;
    uint32_t my_thread_id;
    uint32_t my_unique_id;
    uint32_t my_local_key;
    uint32_t my_remote_key;
    uint32_t total_client_num;

    struct ibv_context   * ib_ctx;
    struct ibv_pd        * ib_pd;
    struct ibv_cq        * ib_cq;   // only for all one-sided operations
    uint8_t                ib_port_num;
    struct ibv_port_attr   ib_port_attr;
    struct ibv_device_attr ib_device_attr;
    union  ibv_gid       * ib_gid;
    struct ibv_comp_channel * ib_ch;
    std::vector<struct ibv_qp *> rc_qp_list;
    std::vector<struct MrInfo *> mr_info_list;

    memcached_st * memc;
    std::unordered_map<std::string, int> called_barrier;
    
    // for UD RPC call
    ibv_qp                * rpc_qp;     // ud or raw packet
    ibv_mr                * rpc_msg_mr;
    ibv_cq                * rpc_send_cq;
    ibv_cq                * rpc_recv_cq;
    void                  * rpc_msg_pool;
    void                  * rpc_send_pool;
    uint16_t                rpc_cur_msg;
    uint16_t                rpc_cur_send;
    ibv_recv_wr           ** rpc_recvs;
    ibv_sge               ** rpc_recv_sgl;
    std::atomic<uint32_t>   rpc_send_counter;
    struct UdAttr         * rem_ud_attr[define::maxNodeNum][define::maxClientNum];
    struct ibv_ah         * rpc_ib_ah[define::maxNodeNum][define::maxClientNum];
    uint32_t                wait_ack_num[define::maxCoroNum];
    struct KVMsg         ** wait_reply[define::maxCoroNum];
    
    ibv_cq        * ckpt_cq;
    bool            is_recovery;

// private methods
private:
    int _init_client(void);
    int _init_server(void);

    struct ibv_qp * _create_rc_qp(struct ibv_cq * send_cq, struct ibv_cq * recv_cq);
    int _get_qp_info(struct ibv_qp * qp, __OUT struct QpInfo * qp_info);

    // general rdma operation
    inline void _fill_sge_wr(ibv_sge &sg, ibv_send_wr &wr, uint64_t source,
                                            uint64_t size, uint32_t lkey);
    int _rdma_write(ibv_qp *qp, uint64_t local_addr, uint64_t remote_addr, uint64_t size,
                    uint32_t local_key, uint32_t remote_key, int32_t imm = -1,
                    bool is_signal = true, uint64_t wr_id = 0);
    bool _rdma_read(ibv_qp *qp, uint64_t local_addr, uint64_t remote_addr, uint64_t size,
                    uint32_t local_key, uint32_t remote_key, bool is_signal = true, uint64_t wr_id = 0);
    bool _rdma_compare_and_swap(ibv_qp *qp, uint64_t local_addr, uint64_t remote_addr,
                                uint64_t compare, uint64_t swap, uint32_t local_key,
                                uint32_t remote_key, bool is_signal = true, uint64_t wr_id = 0);

    // specified rdma operation
    bool _rdma_read_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal = true, uint64_t wr_id = 0);
    bool _rdma_write_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal = true, uint64_t wr_id = 0);
    bool _rdma_mix_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal = true, uint64_t wr_id = 0);
    bool _rdma_send_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal = true, uint64_t wr_id = 0);

    void _mem_create_memc(void);
    void _mem_set(const char *key, uint32_t klen, const char *val, uint32_t vlen);
    char *_mem_get(const char *key, uint32_t klen, size_t *v_size = nullptr);
    uint64_t _mem_fetch_and_add(const char *key, uint32_t klen);

    int _client_connect_one_rc_qp(uint32_t server_id);  // client connect to server
    int _server_publish_ckpt_qps(void); // for ckpt
    int _server_connect_ckpt_qps(void);
    int _server_connect_one_ckpt_qp(uint32_t server_id); // for ckpt, server connect to another server
    int _server_connect_recover_qps(void);
    int _server_connect_one_recover_qp(uint32_t server_id);

    // rdma ud functions
    struct ibv_qp * _create_ud_qp();
    void _rpc_init(void);
    void _rpc_init_recv(void);
    void _rpc_poll_send_cq(void);
    void _rpc_set_ah(char *ud_attr_name, int mc_i, int th_i);
    void _rpc_set_all_ah(void);
    void _rpc_set_server_ah(void);
    void _rpc_set_client_ah(int nd_i, int th_i);

public: 
    NetworkManager(uint8_t role, RdmaContext *rdma_ctx, uint8_t server_id, uint8_t thread_id, uint32_t client_num, ibv_mr *local_mr, bool is_recovery = false);
    ~NetworkManager();

    inline ibv_cq * get_ckpt_cq() {
        return ckpt_cq;
    }

    inline ibv_cq * get_rpc_recv_cq() {
        return rpc_recv_cq;
    }

    inline ibv_cq * get_ib_cq() {
        return ib_cq;
    }

    // common udp functions
    int send_reply(struct KVMsg * reply);
    int send_broad_request(struct KVMsg * request, uint32_t * server_id, int k);
    int send_and_recv_request(struct KVMsg * request, __OUT struct KVMsg ** reply, uint32_t server_id, CoroContext *ctx = nullptr);
    void client_barrier(const std::string &str);
    void client_barrier_prev(const std::string &str, uint32_t sequen_id);
    void client_barrier_post(const std::string &str, uint32_t sequen_id);
    uint32_t get_sequen_id(const std::string &str);

    // rdma
    int client_connect_rc_qps();
    int server_on_connect_new_qp(const struct KVMsg * request, __OUT struct QpInfo * qp_info);
    int server_on_connect_connect_qp(uint32_t client_id, const struct QpInfo * local_qp_info, const struct QpInfo * remote_qp_info);
    int server_get_index_ver(uint32_t server_id, uint64_t ** index_ver);
    int server_set_index_ver(uint64_t index_ver);
    int server_get_compress_size(uint32_t server_id, uint64_t ** compress_size);
    int server_set_compress_size(uint64_t compress_size);

    int rdma_write_sync(uint64_t local_addr, uint64_t remote_addr, uint32_t size, uint32_t server_id);
    int rdma_read_sync(uint64_t local_addr, uint64_t remote_addr, uint32_t size, uint32_t server_id);
    int poll_cq_sync(int poll_number, ibv_cq * poll_cq);
    int poll_cq_once(ibv_wc *wc, ibv_cq * poll_cq);
    int poll_cq_once_sync(struct ibv_wc *wc, ibv_cq * poll_cq);
    int poll_cq_once_block(struct ibv_wc *wc, ibv_cq * poll_cq);

    // for client to use, with CoroContext
    void rdma_write_batch(RdmaOpRegion *rs, int k, bool is_signal = true, CoroContext *ctx = nullptr);
    void rdma_write_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr, ibv_cq * poll_cq = nullptr);
    void rdma_read_batch(RdmaOpRegion *rs, int k, bool is_signal = true, CoroContext *ctx = nullptr);
    void rdma_read_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr, ibv_cq * poll_cq = nullptr);
    int  rdma_read_batches_async(RdmaOpRegion *rs, int k);
    void rdma_mix_batch(RdmaOpRegion *rs, int k, bool is_signal = true, CoroContext *ctx = nullptr);
    void rdma_mix_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx = nullptr);

    // for UD RPC
    void rdma_send_batch(RdmaOpRegion *rs, int k);
    bool rpc_fill_wait_reply(uint32_t coro_id, struct KVMsg *reply);
    char *rpc_get_message();
    char *rpc_get_send_pool();
    
};

#endif