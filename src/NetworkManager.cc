#include "NetworkManager.h"

#include <netdb.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>

#include <boost/fiber/all.hpp>
#include "KvUtils.h"

NetworkManager::NetworkManager(uint8_t role, RdmaContext *rdma_ctx, uint8_t server_id, uint8_t thread_id, uint32_t client_num, ibv_mr *local_mr, bool is_recovery) {
    int ret = 0;
    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    // assert(this->udp_sock_ >= 0);

    this->is_recovery   = is_recovery;
    this->role          = role;
    my_server_id        = server_id;
    my_thread_id        = thread_id;
    my_unique_id        = get_unique_id(server_id, thread_id);
    total_client_num    = client_num;
    
    my_local_key    = local_mr->lkey;
    my_remote_key   = local_mr->rkey;
    conn_type       = define::ibConnType;
    udp_port        = 2333;
    ib_port_num     = rdma_ctx->port;

    // create ib structs
    ib_ctx = rdma_ctx->ctx;
    ib_pd = rdma_ctx->pd;

    ret = ibv_query_port(ib_ctx, ib_port_num, &ib_port_attr);
    assert(ret == 0);

    if (ib_port_attr.state != IBV_PORT_ACTIVE) {
		printf("Warning, port state is %d.\n", ib_port_attr.state);
	}

    ret = ibv_query_device(ib_ctx, &ib_device_attr);
    assert(ret == 0);

    if (conn_type == ROCE) {
        ib_gid = (union ibv_gid *)malloc(sizeof(union ibv_gid));
        ret = ibv_query_gid(ib_ctx, ib_port_num, rdma_ctx->gid_index, ib_gid);
        assert(ret == 0);
    } else {
        assert(conn_type == IB);
        ib_gid = NULL;
    }

    _mem_create_memc();


    if (role == CLIENT)
        _init_client();
    else
        _init_server();
}

int NetworkManager::_init_client(void) {
    assert(this->role == CLIENT);

    struct timeval timeout;
    timeout.tv_sec = 1; // s
    timeout.tv_usec = 0; // us
    setsockopt(udp_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval));
    
    this->num_memory = define::memoryNodeNum;
    this->server_addr_list = (struct sockaddr_in *)malloc(this->num_memory * sizeof(struct sockaddr_in));
    assert(this->server_addr_list != NULL);
    memset(this->server_addr_list, 0, sizeof(struct sockaddr_in) * this->num_memory);

    this->rc_qp_list.resize(define::maxNodeNum);
    this->mr_info_list.reserve(define::maxNodeNum);
    this->ib_cq    = ibv_create_cq(this->ib_ctx, 1024, NULL, NULL, 0);
    assert(this->ib_cq != NULL);

    for (int i = 0; i < define::memoryNodeNum; i ++) {
        server_addr_list[i].sin_family = AF_INET;
        server_addr_list[i].sin_port   = htons(this->udp_port);
        server_addr_list[i].sin_addr.s_addr = inet_addr(define::memoryIPs[i]);
    }

    _rpc_init();
    _rpc_init_recv();
    _rpc_set_server_ah();
    return 0;
}

int NetworkManager::_init_server(void) {
    assert(this->role == SERVER);
    // set sock option
    int ret = 0;
    struct timeval timeout;
    timeout.tv_sec  = 1;
    timeout.tv_usec = 0;
    ret = setsockopt(udp_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval));
    assert(ret == 0);

    this->num_memory = define::memoryNodeNum;
    server_addr_list = (struct sockaddr_in *)malloc(this->num_memory * sizeof(struct sockaddr_in));
    assert(server_addr_list != NULL);
    memset(server_addr_list, 0, sizeof(struct sockaddr_in) * this->num_memory);

    this->ib_cq = ibv_create_cq(this->ib_ctx, 128, NULL, NULL, 0);
    assert(this->ib_cq != NULL);

    for (int i = 0; i < define::memoryNodeNum; i ++) {
        server_addr_list[i].sin_family = AF_INET;
        server_addr_list[i].sin_port   = htons(this->udp_port);
        server_addr_list[i].sin_addr.s_addr = inet_addr(define::memoryIPs[i]);
    }

    struct sockaddr_in server_listen_addr;
    server_listen_addr.sin_family = AF_INET;
    server_listen_addr.sin_port   = htons(this->udp_port);
    server_listen_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    ret = bind(udp_sock, (struct sockaddr *)&server_listen_addr, sizeof(struct sockaddr_in));
    assert(ret >= 0);

    // 
    _rpc_init();
    _rpc_init_recv();
    _rpc_set_server_ah();

    if (!is_recovery) {
        _server_publish_ckpt_qps();
        _server_connect_ckpt_qps();
    } else {
        _server_connect_recover_qps();  // after recovery, recover qps will be new ckpt qps
    }

    return 0;
}

NetworkManager::~NetworkManager() {
    close(this->udp_sock);
    free(server_addr_list);
}

int NetworkManager::send_reply(struct KVMsg * reply) {
    RdmaOpRegion ror;
    ror.local_addr = (uint64_t)reply;
    ror.remote_sid = reply->nd_id;
    ror.remote_tid = reply->th_id;
    ror.size = sizeof(KVMsg);
    ror.op_code = IBV_WR_SEND;

    rdma_send_batch(&ror, 1);
    return 0;
}

// send a broadcast request
int NetworkManager::send_broad_request(struct KVMsg * request, uint32_t * server_id, int k) {
    RdmaOpRegion * ror = new RdmaOpRegion[k];
    for (int i = 0; i < k; i++) {
        ror[i].local_addr = (uint64_t)request;
        ror[i].remote_sid = server_id[i];
        ror[i].remote_tid = 0;
        ror[i].size = sizeof(KVMsg);
        ror[i].op_code = IBV_WR_SEND;
    }
    rdma_send_batch(ror, k);
    delete[] ror;
    return 0;
}

int NetworkManager::send_and_recv_request(struct KVMsg * request, __OUT struct KVMsg ** reply, uint32_t server_id, CoroContext *ctx) {
    assert(reply != nullptr);
    RdmaOpRegion ror;
    ror.local_addr = (uint64_t)request;
    ror.remote_sid = server_id; 
    ror.remote_tid = 0;         // send to memory node's server, so thread id is 0
    ror.size = sizeof(KVMsg);
    ror.op_code = IBV_WR_SEND;

    rdma_send_batch(&ror, 1);

    if (ctx == nullptr) {
        struct ibv_wc wc;
        poll_cq_once_sync(&wc, rpc_recv_cq);
        assert(wc.opcode == IBV_WC_RECV);
        (*reply) = (KVMsg *)rpc_get_message();
    } else {
        wait_ack_num[ctx->coro_id] = 1;
        wait_reply[ctx->coro_id] = reply;
        (*ctx->yield)(*ctx->master);
    }
    assert((*reply)->th_id == my_thread_id && (*reply)->nd_id == my_server_id);
    return 0;
}

void NetworkManager::_mem_create_memc(void)
{
    memcached_server_st *servers = NULL;
    memcached_return rc;

    memc = memcached_create(NULL);
    const char *registry_ip = define::memcachedIP;

    /* We run the memcached server on the default memcached port */
    servers = memcached_server_list_append(servers, registry_ip, MEMCACHED_DEFAULT_PORT, &rc);
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS)
    {
        printf("Couldn't add memcached server\n");
        exit(-1);
    }
}

void NetworkManager::_mem_set(const char *key, uint32_t klen, const char *val, uint32_t vlen) {
  memcached_return rc;
  while (true) {
    rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400);
  }
}

char *NetworkManager::_mem_get(const char *key, uint32_t klen, size_t *v_size) {
  size_t l;
  char *res;
  uint32_t flags;
  memcached_return rc;

  while (true) {

    res = memcached_get(memc, key, klen, &l, &flags, &rc);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(40 * my_unique_id);
  }

  if (v_size != nullptr) {
    *v_size = l;
  }
  
  return res;
}

uint64_t NetworkManager::_mem_fetch_and_add(const char *key, uint32_t klen) {
  uint64_t res = 0;
  while (true) {
    memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
    if (rc == MEMCACHED_SUCCESS) {
      return res;
    } 
    if ((rc == MEMCACHED_NOTFOUND) && (my_server_id == define::memoryNodeNum) && (my_thread_id == 0)) {
        _mem_set(key, klen, "0", 1);
    } 
    usleep(40 * my_unique_id);
  }
  return 0;
}

void NetworkManager::client_barrier(const std::string &str) {
    assert(total_client_num > 0);
    std::string key = std::string("barrier-") + str + std::to_string(called_barrier[str]);
    called_barrier[str] ++;
    
    uint64_t res = _mem_fetch_and_add(key.c_str(), key.size());
    while (true) {
        uint64_t v = std::stoull(_mem_get(key.c_str(), key.size()));
        if ((v % total_client_num) == 0) {
            return;
        }
    }
}

void NetworkManager::client_barrier_prev(const std::string &str, uint32_t sequen_id) {
    assert(total_client_num > 0);
    std::string key = std::string("barrier-") + str + std::to_string(called_barrier[str]);

    if ((my_server_id == define::memoryNodeNum) && (my_thread_id == 0)) {
        _mem_set(key.c_str(), key.size(), "0", 1);
    } 
    while (true) {
        uint64_t v = std::stoull(_mem_get(key.c_str(), key.size()));
        if (v == sequen_id - 1) {
            break;
        }
    }
}

void NetworkManager::client_barrier_post(const std::string &str, uint32_t sequen_id) {
    assert(total_client_num > 0);
    std::string key = std::string("barrier-") + str + std::to_string(called_barrier[str]);
    called_barrier[str] ++;
    
    while (true) {
        uint64_t v = std::stoull(_mem_get(key.c_str(), key.size()));
        if (v == sequen_id - 1) {
            _mem_fetch_and_add(key.c_str(), key.size());
            break;
        }
    }
}

uint32_t NetworkManager::get_sequen_id(const std::string &str) {
    assert(total_client_num > 0);
    std::string key = std::string("barrier-") + str + std::to_string(called_barrier[str]);
    called_barrier[str] ++;
    
    if ((my_server_id == define::memoryNodeNum) && (my_thread_id == 0)) {
        _mem_set(key.c_str(), key.size(), "0", 1);
    } 
    uint64_t res = _mem_fetch_and_add(key.c_str(), key.size());
    // printf("key %s, res %lu\n", key.c_str(), res);
    return (uint32_t)res;
}

void NetworkManager::_rpc_init(void)
{
    rpc_cur_msg = 0;
    rpc_cur_send = 0;
    rpc_send_counter = 0;

    assert(define::messageNR % define::kBatchCount == 0);
    
    rpc_send_cq = ibv_create_cq(ib_ctx, 128, NULL, NULL, 0);    assert(rpc_send_cq != NULL);
    if (role == SERVER) {
#ifdef ENABLE_RPC_BLOCK
        ib_ch = ibv_create_comp_channel(ib_ctx);
        rpc_recv_cq = ibv_create_cq(ib_ctx, 2048, NULL, ib_ch, 0);    assert(rpc_recv_cq != NULL);
        ibv_req_notify_cq(rpc_recv_cq, 0);
#else
        rpc_recv_cq = ibv_create_cq(ib_ctx, 2048, NULL, NULL, 0);    assert(rpc_recv_cq != NULL);
#endif
    } else {
        rpc_recv_cq = ibv_create_cq(ib_ctx, 2048, NULL, NULL, 0);    assert(rpc_recv_cq != NULL);
    }
    rpc_qp = _create_ud_qp();
    modifyUDtoRTS(rpc_qp, ib_port_num);

    rpc_msg_pool = mmap(NULL, (2 * define::messagePoolNR * define::messageSize), PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    assert(rpc_msg_pool != MAP_FAILED);
    rpc_msg_mr = ibv_reg_mr(ib_pd, rpc_msg_pool, (2 * define::messagePoolNR * define::messageSize), 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    rpc_send_pool = (char *)rpc_msg_pool + define::messagePoolNR * define::messageSize;

    // Publish UD QP info
    UdAttr ud_attr;
    ud_attr.lid = ib_port_attr.lid;
    ud_attr.qp_num = rpc_qp->qp_num;
    if (conn_type == ROCE) {
        memcpy(ud_attr.gid, this->ib_gid, 16);
    }

    char ud_attr_name[256];
    sprintf(ud_attr_name, "udattr-%d-%d", my_server_id, my_thread_id);
    _mem_set(ud_attr_name, strlen(ud_attr_name), (const char *)&ud_attr, sizeof(UdAttr));
}

void NetworkManager::_rpc_init_recv() {
    rpc_recvs = new ibv_recv_wr*[define::kPoolBatchCount];
    rpc_recv_sgl = new ibv_sge*[define::kPoolBatchCount];

    for (int i = 0; i < define::kPoolBatchCount; ++i) {
        rpc_recvs[i] = new ibv_recv_wr[define::subNR];
        rpc_recv_sgl[i] = new ibv_sge[define::subNR];
    }

    for (int k = 0; k < define::kPoolBatchCount; ++k) {
        for (size_t i = 0; i < define::subNR; ++i) {
            auto &s = rpc_recv_sgl[k][i];
            memset(&s, 0, sizeof(s));

            s.addr = (uint64_t)rpc_msg_pool + (k * define::subNR + i) * define::messageSize;
            s.length = define::messageSize;
            s.lkey = rpc_msg_mr->lkey;

            auto &r = rpc_recvs[k][i];
            memset(&r, 0, sizeof(r));

            r.sg_list = &s;
            r.num_sge = 1;
            r.next = (i == define::subNR - 1) ? NULL : &rpc_recvs[k][i + 1];
        }
    }

    struct ibv_recv_wr *bad;
    for (int i = 0; i < define::kBatchCount; ++i) {
        if (ibv_post_recv(rpc_qp, &rpc_recvs[i][0], &bad)) {
            printf("Receive failed.\n");
        }
    }
}

void NetworkManager::_rpc_poll_send_cq(void)
{
    int count = 0;
    ibv_wc wc;  
    do {
        int new_count = ibv_poll_cq(rpc_send_cq, 1, &wc);
        count += new_count;
        if (new_count < 0) {
            printf("Poll Completion failed.\n");
            sleep(5);
            return;
        }
        if (new_count > 0 && wc.status != IBV_WC_SUCCESS) {
            printf("Failed status %s (%d) for wr_id %d\n",
                    ibv_wc_status_str(wc.status), wc.status,
                    (int)wc.wr_id);
            sleep(5);
            return;
        }
    } while (count < 1);

    return;
}

void NetworkManager::_rpc_set_ah(char *ud_attr_name, int mc_i, int th_i) {
    while(1) {
        size_t val_len;
        rem_ud_attr[mc_i][th_i] = (struct UdAttr *)_mem_get(ud_attr_name, strlen(ud_attr_name), &val_len);
        if(val_len > 0) {
            assert(val_len == sizeof(UdAttr));
            break;
        } else {
            sleep(.1);
        }	
    }

    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(struct ibv_ah_attr));
    ah_attr.is_global = 0;
    ah_attr.dlid = rem_ud_attr[mc_i][th_i]->lid;
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.port_num = ib_port_num; // Local port
    if (conn_type == ROCE) {
        ah_attr.is_global = 1;
        ah_attr.port_num  = ib_port_num;
        memcpy(&ah_attr.grh.dgid, rem_ud_attr[mc_i][th_i]->gid, 16);
        ah_attr.grh.flow_label = 0;
        ah_attr.grh.hop_limit  = 1;
        ah_attr.grh.sgid_index = define::ibGidIdx;
        ah_attr.grh.traffic_class = 0;
    }

    rpc_ib_ah[mc_i][th_i] = ibv_create_ah(ib_pd, &ah_attr);
    assert(rpc_ib_ah[mc_i][th_i] != NULL);
}

void NetworkManager::_rpc_set_all_ah(void) {
    assert(0);
}

void NetworkManager::_rpc_set_server_ah(void) {
    for(int mc_i = 0; mc_i < define::memoryNodeNum; mc_i++) {
        int th_i = 0;
        char ud_attr_name[256];
        sprintf(ud_attr_name, "udattr-%d-%d", mc_i, th_i);
        _rpc_set_ah(ud_attr_name, mc_i, th_i);
	}
}

void NetworkManager::_rpc_set_client_ah(int nd_i, int th_i) {
    char ud_attr_name[256];
    sprintf(ud_attr_name, "udattr-%d-%d", nd_i, th_i);
    _rpc_set_ah(ud_attr_name, nd_i, th_i);       
}

char *NetworkManager::rpc_get_message() {
    struct ibv_recv_wr *bad;
    char *m = (char *)rpc_msg_pool + rpc_cur_msg * define::messageSize + define::recvPadding;

    ADD_ROUND(rpc_cur_msg, define::messagePoolNR);
    if (rpc_cur_msg % define::subNR == 0) {
        if (ibv_post_recv(
                rpc_qp,
                &rpc_recvs[(define::kPoolBatchCount + (rpc_cur_msg + define::messageNR) / define::subNR - 1) % define::kPoolBatchCount][0],
                &bad)) {
            printf("Receive failed.\n");
        }
    }
    return m;
}

char *NetworkManager::rpc_get_send_pool() {
    char *s = (char *)rpc_send_pool + rpc_cur_send * define::messageSize + define::sendPadding;

    ADD_ROUND(rpc_cur_send, define::messagePoolNR);

    return s;
}

struct ibv_qp * NetworkManager::_create_ud_qp() {
    struct ibv_exp_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_type = IBV_QPT_UD;
    attr.sq_sig_all = 0;
    attr.send_cq = rpc_send_cq;
    attr.recv_cq = rpc_recv_cq;
    attr.pd = this->ib_pd;

    attr.comp_mask = IBV_EXP_QP_INIT_ATTR_PD;

    attr.cap.max_send_wr = define::messageNR;
    attr.cap.max_recv_wr = define::messageNR;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = define::rdmaInlineSize;

    struct ibv_qp *new_qp = ibv_exp_create_qp(this->ib_ctx, &attr);
    if (!new_qp) {
        printf("Failed to create UD QP\n");
        return NULL;
    }

    return new_qp;
}

struct ibv_qp * NetworkManager::_create_rc_qp(struct ibv_cq * send_cq, struct ibv_cq * recv_cq) {
    struct ibv_exp_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;
    attr.send_cq = send_cq;
    attr.recv_cq = recv_cq;
    attr.pd = this->ib_pd;

    attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS |
                    IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG;
    attr.max_atomic_arg = 8;
    
    attr.cap.max_send_wr = 512;
    attr.cap.max_recv_wr = 1;
    attr.cap.max_send_sge = 16;
    attr.cap.max_recv_sge = 16;
    attr.cap.max_inline_data = define::rdmaInlineSize;

    struct ibv_qp *new_qp = ibv_exp_create_qp(this->ib_ctx, &attr);
    if (!new_qp) {
        printf("Failed to create RC QP\n");
        return NULL;
    }
    return new_qp;
}

int NetworkManager::client_connect_rc_qps() {
    int ret = 0;
    for (int i = 0; i < define::memoryNodeNum; i ++) {
        ret = _client_connect_one_rc_qp(i);
        assert(ret == 0);
    }
    return 0;
}

int NetworkManager::_client_connect_one_rc_qp(uint32_t server_id) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = _create_rc_qp(this->ib_cq, this->ib_cq);
    
    struct KVMsg * request;
    struct KVMsg * reply;

    request = (struct KVMsg *)rpc_get_send_pool();

    request->type = REQ_CONNECT;
    request->nd_id = my_server_id;
    request->th_id = my_thread_id;
    request->co_id = 0;
    rc = _get_qp_info(new_rc_qp, &(request->body.conn_info.qp_info));

    send_and_recv_request(request, &reply, server_id);
    assert(reply->type == REP_CONNECT);
    
    rc = ib_connect_qp(new_rc_qp, &(request->body.conn_info.qp_info), 
        &(reply->body.conn_info.qp_info), conn_type, CLIENT);
    assert(rc == 0);

    // record this rc_qp
    struct MrInfo * new_mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    memcpy(new_mr_info, &(reply->body.conn_info.gc_info), sizeof(struct MrInfo));
    rc_qp_list[server_id] = new_rc_qp;
    mr_info_list[server_id] = new_mr_info;
    return 0;
}

int NetworkManager::_get_qp_info(struct ibv_qp * qp, __OUT struct QpInfo * qp_info) {
    qp_info->qp_num   = qp->qp_num;
    qp_info->lid      = this->ib_port_attr.lid;
    qp_info->port_num = this->ib_port_num;
    if (this->conn_type == ROCE) {
        memcpy(qp_info->gid, this->ib_gid, sizeof(union ibv_gid));
    } else {
        memset(qp_info->gid, 0, sizeof(union ibv_gid));
    }
    return 0;
}

inline void NetworkManager::_fill_sge_wr(ibv_sge &sg, ibv_send_wr &wr, uint64_t source,
                                                uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

int NetworkManager::_rdma_write(ibv_qp *qp, uint64_t local_addr, uint64_t remote_addr, uint64_t size,
                                uint32_t local_key, uint32_t remote_key, int32_t imm,
                                bool is_signal, uint64_t wr_id) {
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    _fill_sge_wr(sg, wr, local_addr, size, local_key);

    if (imm == -1) {
        wr.opcode = IBV_WR_RDMA_WRITE;
    } else {
        wr.imm_data = imm;
        wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    }

    if (is_signal) {
        wr.send_flags = IBV_SEND_SIGNALED;
    }
    if (size <= define::rdmaInlineSize) {  // Optimization
        wr.send_flags |= IBV_SEND_INLINE;
    }

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_key;
    wr.wr_id = wr_id;

    if (ibv_post_send(qp, &wr, &wrBad) != 0) {
        printf("Send with RDMA_WRITE(WITH_IMM) failed.\n");
        sleep(10);
        return false;
    }
    return true;
}

// for RC & UC
bool NetworkManager::_rdma_read(ibv_qp *qp, uint64_t local_addr, uint64_t remote_addr, uint64_t size,
                                uint32_t local_key, uint32_t remote_key, bool is_signal, uint64_t wr_id) {
  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  _fill_sge_wr(sg, wr, local_addr, size, local_key);

  wr.opcode = IBV_WR_RDMA_READ;

  if (is_signal) {
    wr.send_flags = IBV_SEND_SIGNALED;
  }

  wr.wr.rdma.remote_addr = remote_addr;
  wr.wr.rdma.rkey = remote_key;
  wr.wr_id = wr_id;

  if (ibv_post_send(qp, &wr, &wrBad)) {
    printf("Send with RDMA_READ failed.\n");
    return false;
  }
  return true;
}

bool NetworkManager::_rdma_compare_and_swap(ibv_qp *qp, uint64_t source, uint64_t dest,
                                            uint64_t compare, uint64_t swap, uint32_t lkey,
                                            uint32_t remoteRKey, bool signal, uint64_t wrID) {
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *wrBad;

    _fill_sge_wr(sg, wr, source, 8, lkey);

    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

    if (signal) {
        wr.send_flags = IBV_SEND_SIGNALED;
    }

    wr.wr.atomic.remote_addr = dest;
    wr.wr.atomic.rkey = remoteRKey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;
    wr.wr_id = wrID;

    if (ibv_post_send(qp, &wr, &wrBad)) {
        printf("Send with ATOMIC_CMP_AND_SWP failed.\n");
        sleep(5);
        return false;
    }
    return true;
}

int NetworkManager::poll_cq_sync(int poll_number, ibv_cq * poll_cq) {
    assert(poll_cq != nullptr);
    int count = 0;
    ibv_wc wc;  
    do {
        int new_count = ibv_poll_cq(poll_cq, 1, &wc);
        count += new_count;
        if (new_count < 0) {
            printf("Poll Completion failed.\n");
            sleep(5);
            return -1;
        }
        if (new_count > 0 && wc.status != IBV_WC_SUCCESS) {
            printf("Failed status %s (%d) for wr_id %d\n",
                    ibv_wc_status_str(wc.status), wc.status,
                    (int)wc.wr_id);
            sleep(5);
            return -1;
        }
    } while (count < poll_number);

    return count;
}

int NetworkManager::poll_cq_once_sync(struct ibv_wc *wc, ibv_cq * poll_cq) {
    assert(wc != nullptr);
    int count = 0;
    do {
        int new_count = ibv_poll_cq(poll_cq, 1, wc);
        count += new_count;
        if (new_count < 0) {
            printf("Poll Completion failed.\n");
            sleep(5);
            return -1;
        }
        if (new_count > 0 && wc->status != IBV_WC_SUCCESS) {
            printf("Failed status %s (%d) for wr_id %d\n",
                    ibv_wc_status_str(wc->status), wc->status,
                    (int)wc->wr_id);
            sleep(5);
            return -1;
        }
    } while (count == 0);
    if (count > 1) {
        printf("poll count > 1!\n");
    }
    return count;
}

int NetworkManager::poll_cq_once_block(struct ibv_wc *wc, ibv_cq * poll_cq) {
    assert(this->role == SERVER);
    assert(this->ib_ch != nullptr);
    assert(wc != nullptr);
    int ret = 0, new_count = 0;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;

    new_count = ibv_poll_cq(poll_cq, 1, wc);
    if (new_count < 0) {
        printf("Poll Completion failed.\n");
        sleep(5);
        return -1;
    }
    if (new_count > 0 && wc->status != IBV_WC_SUCCESS) {
        printf("Failed status %s (%d) for wr_id %d\n",
                ibv_wc_status_str(wc->status), wc->status,
                (int)wc->wr_id);
        sleep(5);
        return -1;
    }
    if (new_count == 1) {
        return new_count;
    }

// wait (only new_count == 0 will reach here)
    ret = ibv_get_cq_event(this->ib_ch, &ev_cq, &ev_ctx);
    if (ret) {
        printf("Failed to get CQ event.\n");
        return 0;
    }
    /* Ack the event */
    ibv_ack_cq_events(ev_cq, 1);
    
    /* Request notification upon the next completion event */
    ret = ibv_req_notify_cq(ev_cq, 0);
    if (ret) {
        printf("Couldn't request CQ notification.\n");
        return 0;
    }
    return 0;
}

int NetworkManager::poll_cq_once(ibv_wc *wc, ibv_cq * poll_cq) {
    assert(wc != nullptr);
    int count = ibv_poll_cq(poll_cq, 1, wc);
    if (count <= 0) {
        return 0;
    }
    if (wc->status != IBV_WC_SUCCESS) {
        printf("Failed status %s (%d) for wr_id %d\n",
                ibv_wc_status_str(wc->status), wc->status,
                (int)wc->wr_id);
        return -1;
    } else {
        return count;
    }
}

int NetworkManager::server_on_connect_new_qp(const struct KVMsg * request, __OUT struct QpInfo * qp_info) {
    int rc = 0;
    struct ibv_cq * poll_cq = nullptr;
    if (request->nd_id < define::memoryNodeNum) 
        poll_cq = this->ckpt_cq;
    else
        poll_cq = this->ib_cq;
    struct ibv_qp * new_rc_qp = _create_rc_qp(poll_cq, poll_cq);
    assert(new_rc_qp != NULL);
    
    uint32_t unique_id = get_unique_id(request->nd_id, request->th_id); 

    _rpc_set_client_ah(request->nd_id, request->th_id);

    if (this->rc_qp_list.size() <= unique_id) {
        this->rc_qp_list.resize(unique_id + 1);
    }
    if (this->rc_qp_list[unique_id] != NULL) {
        ibv_destroy_qp(rc_qp_list[unique_id]);
    }
    this->rc_qp_list[unique_id] = new_rc_qp;

    rc = _get_qp_info(new_rc_qp, qp_info);
    return 0;
}

int NetworkManager::server_on_connect_connect_qp(uint32_t client_id, 
                                                const struct QpInfo * local_qp_info, 
                                                const struct QpInfo * remote_qp_info) {
    int rc = 0;
    struct ibv_qp * qp = this->rc_qp_list[client_id];
    rc = ib_connect_qp(qp, local_qp_info, remote_qp_info, this->conn_type, this->role);
    assert(rc == 0);
    return 0;
}

int NetworkManager::server_get_index_ver(uint32_t server_id, uint64_t ** index_ver) {
    char time_key[128] = {0}; 
    sprintf(time_key, "server-time-%d", server_id);

    while(1) {
        size_t val_len;
        (*index_ver) = (uint64_t *)_mem_get(time_key, strlen(time_key), &val_len);
        if(val_len > 0) {
            assert(val_len == sizeof(uint64_t));
            break;
        } else {
            sleep(.1);
        }	
    }
    return 0;
}

int NetworkManager::server_set_index_ver(uint64_t index_ver) {
    char time_key[128] = {0}; 
    sprintf(time_key, "server-time-%d", my_server_id);
    _mem_set(time_key, strlen(time_key), (const char *)&index_ver, sizeof(uint64_t));
    return 0;
}

int NetworkManager::server_get_compress_size(uint32_t server_id, uint64_t ** compress_size) {
    char size_key[128] = {0}; 
    sprintf(size_key, "server-index-zsize-%d", server_id);

    while(1) {
        size_t val_len;
        (*compress_size) = (uint64_t *)_mem_get(size_key, strlen(size_key), &val_len);
        if(val_len > 0) {
            assert(val_len == sizeof(uint64_t));
            break;
        } else {
            sleep(.1);
        }	
    }
    return 0;
}

int NetworkManager::server_set_compress_size(uint64_t compress_size) {
    char size_key[128] = {0}; 
    sprintf(size_key, "server-index-zsize-%d", my_server_id);
    _mem_set(size_key, strlen(size_key), (const char *)&compress_size, sizeof(uint64_t));
    return 0;
}

int NetworkManager::rdma_write_sync(uint64_t local_addr, uint64_t remote_addr, 
                                    uint32_t size, uint32_t server_id) {
    struct ibv_qp * send_qp = this->rc_qp_list[server_id];
    _rdma_write(send_qp, local_addr, remote_addr, size, my_local_key, mr_info_list[server_id]->rkey);
    
    ibv_wc wc;
    poll_cq_sync(1, ib_cq);
    return 0;
}

int NetworkManager::rdma_read_sync(uint64_t local_addr, uint64_t remote_addr, 
                                    uint32_t size, uint32_t server_id) {
    struct ibv_qp * send_qp = this->rc_qp_list[server_id];
    _rdma_read(send_qp, local_addr, remote_addr, size, my_local_key, mr_info_list[server_id]->rkey);
    poll_cq_sync(1, ib_cq);
    return 0;
}

bool NetworkManager::_rdma_read_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal, uint64_t wr_id) {
    struct ibv_sge sg[kReadOroMax];
    struct ibv_send_wr wr[kReadOroMax];
    struct ibv_send_wr *wrBad;
    assert(k <= kReadOroMax);

    for (int i = 0; i < k; ++i) {
        _fill_sge_wr(sg[i], wr[i], ror[i].local_addr, ror[i].size, my_local_key);

        wr[i].next = (i == k - 1) ? NULL : &wr[i + 1];
        wr[i].opcode = IBV_WR_RDMA_READ;

        if (i == k - 1 && is_signal) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
        }

        wr[i].wr.rdma.remote_addr = ror[i].remote_addr;
        wr[i].wr.rdma.rkey = mr_info_list[ror[i].remote_sid]->rkey;
        wr[i].wr_id = wr_id;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad) != 0) {
        printf("Send with RDMA_READ(WITH_IMM) failed.\n");
        sleep(10);
        return false;
    }
    return true;
}


bool NetworkManager::_rdma_write_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal, uint64_t wr_id) {
    struct ibv_sge sg[kWriteOroMax];
    struct ibv_send_wr wr[kWriteOroMax];
    struct ibv_send_wr *wrBad;
    assert(k <= kWriteOroMax);

    for (int i = 0; i < k; ++i) {
        _fill_sge_wr(sg[i], wr[i], ror[i].local_addr, ror[i].size, my_local_key);

        wr[i].next = (i == k - 1) ? NULL : &wr[i + 1];
        wr[i].opcode = IBV_WR_RDMA_WRITE;

        if (i == k - 1 && is_signal) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
        }
        if (ror[i].size <= define::rdmaInlineSize) {  // Optimization
            wr[i].send_flags |= IBV_SEND_INLINE;
        }

        wr[i].wr.rdma.remote_addr = ror[i].remote_addr;
        wr[i].wr.rdma.rkey = mr_info_list[ror[i].remote_sid]->rkey;
        wr[i].wr_id = wr_id;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad) != 0) {
        printf("Send with RDMA_WRITE(WITH_IMM) failed.\n");
        sleep(10);
        return false;
    }
    return true;
}

bool NetworkManager::_rdma_mix_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal, uint64_t wr_id) {
    struct ibv_sge sg[kWriteOroMax];
    struct ibv_send_wr wr[kWriteOroMax];
    struct ibv_send_wr *wrBad;
    assert(k <= kWriteOroMax);

    for (int i = 0; i < k; ++i) {
        _fill_sge_wr(sg[i], wr[i], ror[i].local_addr, ror[i].size, my_local_key);

        wr[i].next = (i == k - 1) ? NULL : &wr[i + 1];
        wr[i].opcode = ror[i].op_code;

        if (i == k - 1 && is_signal) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
        }
        if (ror[i].size <= define::rdmaInlineSize && ror[i].op_code == IBV_WR_RDMA_WRITE) {  // Optimization
            wr[i].send_flags |= IBV_SEND_INLINE;
        }
        if (ror[i].op_code == IBV_WR_ATOMIC_CMP_AND_SWP) {
            wr[i].wr.atomic.remote_addr = ror[i].remote_addr;
            wr[i].wr.atomic.rkey = mr_info_list[ror[i].remote_sid]->rkey;
            wr[i].wr.atomic.compare_add = ror[i].compare_val;
            wr[i].wr.atomic.swap = ror[i].swap_val;
        } else {
            wr[i].wr.rdma.remote_addr = ror[i].remote_addr;
            wr[i].wr.rdma.rkey = mr_info_list[ror[i].remote_sid]->rkey;
        }
        wr[i].wr_id = wr_id;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad) != 0) {
        printf("Send with RDMA_MIX failed.\n");
        sleep(10);
        return false;
    }
    return true;
}

// for RPC call
bool NetworkManager::_rdma_send_batch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool is_signal, uint64_t wr_id) {
    struct ibv_sge sg[kWriteOroMax];
    struct ibv_send_wr wr[kWriteOroMax];
    struct ibv_send_wr *wrBad;
    assert(k <= kWriteOroMax);

    for (int i = 0; i < k; ++i) {
        _fill_sge_wr(sg[i], wr[i], ror[i].local_addr, ror[i].size, rpc_msg_mr->lkey);
        
        if (my_server_id < define::memoryNodeNum && ror[i].remote_sid >= define::memoryNodeNum) {
            struct KVMsg * reply = (struct KVMsg *)ror[i].local_addr;
            assert(reply->nd_id == ror[i].remote_sid);
            assert(reply->th_id == ror[i].remote_tid);
        }
        wr[i].next = (i == k - 1) ? NULL : &wr[i + 1];
        wr[i].opcode = IBV_WR_SEND;

        if (i == k - 1 && is_signal) {
            wr[i].send_flags = IBV_SEND_SIGNALED;
        }
        if (ror[i].size <= define::rdmaInlineSize) {  // Optimization
            wr[i].send_flags |= IBV_SEND_INLINE;
        }

        wr[i].wr.ud.ah = rpc_ib_ah[ror[i].remote_sid][ror[i].remote_tid];
        wr[i].wr.ud.remote_qpn = rem_ud_attr[ror[i].remote_sid][ror[i].remote_tid]->qp_num;
        wr[i].wr.ud.remote_qkey = UD_PKEY;
        wr[i].wr_id = wr_id;
    }

    if (ibv_post_send(qp, &wr[0], &wrBad) != 0) {
        printf("Send with RDMA_SEND failed.\n");
        sleep(10);
        return false;
    }
    return true;
}

void NetworkManager::rdma_write_batch(RdmaOpRegion *rs, int k, bool is_signal, CoroContext *ctx) {
    uint32_t server_id = rs[0].remote_sid;
    struct ibv_qp * send_qp = this->rc_qp_list[server_id];
    if (ctx == nullptr) {
        _rdma_write_batch(send_qp, rs, k, is_signal, 0xFFFF);
    } else {
        _rdma_write_batch(send_qp, rs, k, true, ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void NetworkManager::rdma_write_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx, ibv_cq * poll_cq) {
    assert(k <= kWriteOroMax);
    RdmaOpRegion each_rs[kMachineMax][kWriteOroMax];
    int cnt[kMachineMax];

    std::fill(cnt, cnt + kMachineMax, 0);
    for (int i = 0; i < k; ++ i) {
        uint32_t server_id = rs[i].remote_sid;
        each_rs[server_id][cnt[server_id] ++] = rs[i];
    }
    int poll_num = 0;
    for (int i = 0; i < kMachineMax; ++ i) if (cnt[i] > 0) {
        rdma_write_batch(each_rs[i], cnt[i], true, ctx);
        poll_num ++;
    }

    if (poll_cq == nullptr) {
        poll_cq = this->ib_cq;
    }
    if (ctx == nullptr) {
        poll_cq_sync(poll_num, poll_cq);
    }
}

void NetworkManager::rdma_read_batch(RdmaOpRegion *rs, int k, bool is_signal, CoroContext *ctx) {
    uint32_t server_id = rs[0].remote_sid;
    struct ibv_qp * send_qp = this->rc_qp_list[server_id];
    if (ctx == nullptr) {
        _rdma_read_batch(send_qp, rs, k, is_signal, 0xFFFF);
    } else {
        _rdma_read_batch(send_qp, rs, k, true, ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void NetworkManager::rdma_read_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx, ibv_cq * poll_cq) {
    RdmaOpRegion each_rs[kMachineMax][kReadOroMax];
    int cnt[kMachineMax];

    int i = 0;
    int poll_num = 0;
    while (i < k) {
        std::fill(cnt, cnt + kMachineMax, 0);
        while (i < k) {
            uint32_t server_id = rs[i].remote_sid;
            each_rs[server_id][cnt[server_id] ++] = rs[i];
            i ++;
            if (cnt[server_id] >= kReadOroMax) break;
        }
        for (int j = 0; j < kMachineMax; ++ j) if (cnt[j] > 0) {
            rdma_read_batch(each_rs[j], cnt[j], true, ctx);
            poll_num ++;
        }
    }

    if (poll_cq == nullptr) {
        poll_cq = this->ib_cq;
    }
    if (ctx == nullptr) {
        poll_cq_sync(poll_num, poll_cq);
    }
}

int NetworkManager::rdma_read_batches_async(RdmaOpRegion *rs, int k) {
    RdmaOpRegion each_rs[kMachineMax][kReadOroMax];
    int cnt[kMachineMax];

    int i = 0;
    int poll_num = 0;
    while (i < k) {
        std::fill(cnt, cnt + kMachineMax, 0);
        while (i < k) {
            uint32_t server_id = rs[i].remote_sid;
            each_rs[server_id][cnt[server_id] ++] = rs[i];
            i ++;
            if (cnt[server_id] >= kReadOroMax) break;
        }
        for (int j = 0; j < kMachineMax; ++ j) if (cnt[j] > 0) {
            rdma_read_batch(each_rs[j], cnt[j], true);
            poll_num ++;
        }
    }

    return poll_num;
}

void NetworkManager::rdma_mix_batch(RdmaOpRegion *rs, int k, bool is_signal, CoroContext *ctx) {
    uint32_t server_id = rs[0].remote_sid;
    assert(server_id < define::memoryNodeNum);  
    struct ibv_qp * send_qp = this->rc_qp_list[server_id];

    if (ctx == nullptr) {
        _rdma_mix_batch(send_qp, rs, k, is_signal, 0xFFFF);
    } else {
        _rdma_mix_batch(send_qp, rs, k, true, ctx->coro_id);
        (*ctx->yield)(*ctx->master);
    }
}

void NetworkManager::rdma_mix_batches_sync(RdmaOpRegion *rs, int k, CoroContext *ctx) {
    assert(k <= kWriteOroMax);
    RdmaOpRegion each_rs[kMachineMax][kWriteOroMax];
    int cnt[kMachineMax];

    std::fill(cnt, cnt + kMachineMax, 0);
    for (int i = 0; i < k; ++ i) {
        uint32_t server_id = rs[i].remote_sid;
        each_rs[server_id][cnt[server_id] ++] = rs[i];
    }
    int poll_num = 0;
    for (int i = 0; i < kMachineMax; ++ i) if (cnt[i] > 0) {
        rdma_mix_batch(each_rs[i], cnt[i], true, ctx);
        poll_num ++;
    }

    if (ctx == nullptr) {
        poll_cq_sync(poll_num, ib_cq);
    }
}

void NetworkManager::rdma_send_batch(RdmaOpRegion *rs, int k) {
    assert(k <= kWriteOroMax);
    uint32_t send_counter = rpc_send_counter.fetch_add(1);
    if ((send_counter & SIGNAL_BATCH) == 0 && send_counter > 0) {
        _rpc_poll_send_cq();
    }
    _rdma_send_batch(rpc_qp, rs, k, (send_counter & SIGNAL_BATCH) == 0);
}

bool NetworkManager::rpc_fill_wait_reply(uint32_t coro_id, struct KVMsg *reply) {
    assert(wait_ack_num[coro_id] == 1);
    assert(coro_id < define::maxCoroNum);

    (*wait_reply[coro_id]) = reply;
    wait_ack_num[coro_id]--;
    if (wait_ack_num[coro_id] == 0)
        return true;
    return false;
}

int NetworkManager::_server_publish_ckpt_qps(void) {
    ckpt_cq = ibv_create_cq(ib_ctx, 128, NULL, NULL, 0);
    assert(ckpt_cq != NULL);
    MrInfo my_mr_info = {define::baseAddr, this->my_remote_key};
    printf("[SERVER TO SERVER] my addr %lx, rkey %x\n", define::baseAddr, this->my_remote_key);
    char mr_key[128] = {0}; 
    sprintf(mr_key, "server-mr-%d", my_server_id);
    _mem_set(mr_key, strlen(mr_key), (const char *)&my_mr_info, sizeof(MrInfo));

    for (int i = 1; i <= (define::memoryNodeNum - 1); i++) {
        uint32_t tmp_sid = (my_server_id + i) % define::memoryNodeNum;
        struct ibv_qp * tmp_qp = _create_rc_qp(ckpt_cq, ckpt_cq);
        assert(tmp_qp != NULL);
        struct QpInfo tmp_qp_info;
        _get_qp_info(tmp_qp, &tmp_qp_info);
        char tmp_qp_key[128] = {0};
        sprintf(tmp_qp_key, "server-to-server-qp-%d-%d", my_server_id, tmp_sid);
        _mem_set(tmp_qp_key, strlen(tmp_qp_key), (const char *)&tmp_qp_info, sizeof(QpInfo));

        if (this->rc_qp_list.size() <= tmp_sid) {
            this->rc_qp_list.resize(tmp_sid + 1);
        }
        if (this->mr_info_list.size() <= tmp_sid) {
            this->mr_info_list.resize(tmp_sid + 1);
        }
        if (this->rc_qp_list[tmp_sid] != NULL) {
            ibv_destroy_qp(rc_qp_list[tmp_sid]);
        }
        rc_qp_list[tmp_sid] = tmp_qp;
    }

    uint64_t init_time = 0;
    char time_key[128] = {0}; 
    sprintf(time_key, "server-time-%d", my_server_id);
    _mem_set(time_key, strlen(time_key), (const char *)&init_time, sizeof(uint64_t));
}

int NetworkManager::_server_connect_ckpt_qps(void) {
    for (uint32_t i = 1; i <= (define::memoryNodeNum - 1); i++) {
        uint32_t tmp_sid = (my_server_id + i) % define::memoryNodeNum;
        _server_connect_one_ckpt_qp(tmp_sid);
    }
}

int NetworkManager::_server_connect_one_ckpt_qp(uint32_t server_id) {
    int rc = 0;
    
    struct ibv_qp * local_qp = rc_qp_list[server_id];
    assert(local_qp != NULL);
    struct QpInfo local_qp_info;
    _get_qp_info(local_qp, &local_qp_info);

    struct QpInfo * remote_qp_info;
    char remote_qp_key[128] = {0};
    sprintf(remote_qp_key, "server-to-server-qp-%d-%d", server_id, my_server_id);
    while(1) {
        size_t val_len;
        remote_qp_info = (struct QpInfo *)_mem_get(remote_qp_key, strlen(remote_qp_key), &val_len);
        if(val_len > 0) {
            assert(val_len == sizeof(QpInfo));
            break;
        } else {
            sleep(.1);
        }	
    }

    rc = ib_connect_qp(local_qp, &local_qp_info, 
        remote_qp_info, conn_type, CLIENT); // here the server is CLIENT
    assert(rc == 0);

    struct MrInfo * remote_mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    struct MrInfo * tmp_remote_mr_info;
    char remote_mr_key[128] = {0};
    sprintf(remote_mr_key, "server-mr-%d", server_id);
    while(1) {
        size_t val_len;
        tmp_remote_mr_info = (struct MrInfo *)_mem_get(remote_mr_key, strlen(remote_mr_key), &val_len);
        if(val_len > 0) {
            assert(val_len == sizeof(MrInfo));
            break;
        } else {
            sleep(.1);
        }	
    }

    memcpy(remote_mr_info, tmp_remote_mr_info, sizeof(struct MrInfo));
    mr_info_list[server_id] = remote_mr_info;
    printf("[SERVER TO SERVER] connect to server(%d) addr(%lx) rkey(%x)\n", server_id, remote_mr_info->addr, remote_mr_info->rkey);
    return 0;
}

int NetworkManager::_server_connect_recover_qps(void) {
    ckpt_cq = ibv_create_cq(ib_ctx, 128, NULL, NULL, 0);
    assert(ckpt_cq != NULL);
    for (uint32_t i = 1; i <= (define::memoryNodeNum - 1); i++) {
        uint32_t tmp_sid = (my_server_id + i) % define::memoryNodeNum;
        _server_connect_one_recover_qp(tmp_sid);
    }
}

int NetworkManager::_server_connect_one_recover_qp(uint32_t server_id) {
    int rc = 0;
    struct ibv_qp * new_rc_qp = _create_rc_qp(this->ckpt_cq, this->ckpt_cq);
    
    struct KVMsg * request;
    struct KVMsg * reply;

    request = (struct KVMsg *)rpc_get_send_pool();

    request->type = REQ_CONNECT;
    request->nd_id = my_server_id;
    request->th_id = my_thread_id;
    request->co_id = 0;
    rc = _get_qp_info(new_rc_qp, &(request->body.conn_info.qp_info));

    send_and_recv_request(request, &reply, server_id);
    assert(reply->type == REP_CONNECT);
    
    rc = ib_connect_qp(new_rc_qp, &(request->body.conn_info.qp_info), 
        &(reply->body.conn_info.qp_info), conn_type, CLIENT); // here the server is CLIENT
    assert(rc == 0);

    // record this rc_qp
    struct MrInfo * new_mr_info = (struct MrInfo *)malloc(sizeof(struct MrInfo));
    memcpy(new_mr_info, &(reply->body.conn_info.gc_info), sizeof(struct MrInfo));

    if (this->rc_qp_list.size() <= server_id) {
        this->rc_qp_list.resize(server_id + 1);
    }
    if (this->mr_info_list.size() <= server_id) {
        this->mr_info_list.resize(server_id + 1);
    }
    if (this->rc_qp_list[server_id] != NULL) {
        ibv_destroy_qp(rc_qp_list[server_id]);
    }

    rc_qp_list[server_id] = new_rc_qp;
    mr_info_list[server_id] = new_mr_info;
    return 0;
}