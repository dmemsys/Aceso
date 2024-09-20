#include "Rdma.h"
#include "Common.h"

#include <infiniband/verbs.h>

static int modify_qp_to_init(struct ibv_qp * qp, const struct QpInfo * local_qp_info) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = local_qp_info->port_num;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    attr_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

static int modify_qp_to_rtr(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info,
    const struct QpInfo * remote_qp_info,
    uint8_t conn_type) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state    = IBV_QPS_RTR;
    attr.path_mtu    = (ibv_mtu)define::ibPathMtu;
    attr.dest_qp_num = remote_qp_info->qp_num;
    attr.rq_psn      = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid  = remote_qp_info->lid;
    attr.ah_attr.sl    = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = local_qp_info->port_num;
    if (conn_type == ROCE) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num  = local_qp_info->port_num;
        memcpy(&attr.ah_attr.grh.dgid, remote_qp_info->gid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit  = 1;
        attr.ah_attr.grh.sgid_index = local_qp_info->gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(local_qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

static int modify_qp_to_rts(struct ibv_qp * local_qp) {
    struct ibv_qp_attr attr;
    int    attr_mask;
    int    rc;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;
    attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(local_qp, &attr, attr_mask);
    // assert(rc == 0);
    return 0;
}

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, 
    uint8_t conn_type, uint8_t role) {
    int rc = 0;
    rc = modify_qp_to_init(local_qp, local_qp_info);
    assert(rc == 0);

    rc = modify_qp_to_rtr(local_qp, local_qp_info, remote_qp_info, conn_type);
    assert(rc == 0);

    if (role == SERVER) {
        return 0;
    }
    
    rc = modify_qp_to_rts(local_qp);
    assert(rc == 0);
    return 0;
}

static struct ibv_context * ib_get_ctx(uint32_t port_id) {
    struct ibv_device ** ib_dev_list = NULL;
    struct ibv_device ** ib_dev_list_iter = NULL;
    struct ibv_device *  ib_dev = NULL;
    int    num_device;

    ib_dev_list = ibv_get_device_list(&num_device);
    assert(ib_dev_list != NULL);
    ib_dev_list_iter = ib_dev_list;

    for (; (*ib_dev_list_iter); ++ib_dev_list_iter) {
        if (!strcmp(ibv_get_device_name(*ib_dev_list_iter), define::ibDevName)) {
            ib_dev = *ib_dev_list_iter;
            break;
        }
    } 
    if (!ib_dev) {
        fprintf(stderr, "IB device %s not found\n", define::ibDevName);
        return NULL;
    }

    struct ibv_context * ret = ibv_open_device(ib_dev);
    ibv_free_device_list(ib_dev_list);
    return ret;
}

bool createContext(RdmaContext *context) {
    int ret = -1;
    ibv_context *ib_ctx = NULL;
    ibv_pd *ib_pd = NULL;
    ibv_port_attr   ib_port_attr;
    ibv_device_attr ib_device_attr;

    // create ib structs
    ib_ctx = ib_get_ctx(define::ibPortId);
    assert(ib_ctx != NULL);

    ib_pd = ibv_alloc_pd(ib_ctx);
    assert(ib_pd != NULL);

    ret = ibv_query_port(ib_ctx, define::ibPortId, &ib_port_attr);
    assert(ret == 0);

    ret = ibv_query_device(ib_ctx, &ib_device_attr);
    assert(ret == 0);

    context->port = define::ibPortId;
    context->gid_index = define::ibGidIdx;
    context->ctx = ib_ctx;
    context->pd = ib_pd;
    context->lid = ib_port_attr.lid;
    if (define::ibConnType == ROCE) {
        ret = ibv_query_gid(ib_ctx, define::ibPortId, define::ibGidIdx, &context->gid);
        assert(ret == 0);
    }

    return true;
}

void *createMemoryRegion(uint64_t base_addr, uint64_t base_len) {
    void *buffer = NULL;
    int port_flag = PROT_READ | PROT_WRITE;
    int mm_flag   = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB;
    buffer = mmap((void *)base_addr, (size_t)base_len, port_flag, mm_flag, -1, 0);
    assert((uint64_t)buffer == base_addr);
    if (buffer == MAP_FAILED) {
        printf("not enough huge-page memory\n");
        return NULL;
    }
    return buffer;
}

bool modifyUDtoRTS(struct ibv_qp *qp, uint8_t port_num) {
    // assert(qp->qp_type == IBV_QPT_UD);

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = port_num;
    attr.qkey = UD_PKEY;

    if (qp->qp_type == IBV_QPT_UD) {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                         IBV_QP_PORT | IBV_QP_QKEY)) {
            printf("Failed to modify QP state to INIT\n");
            return false;
        }
    } else {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PORT)) {
            printf("Failed to modify QP state to INIT\n");
            return false;
        }
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
        printf("failed to modify QP state to RTR\n");
        return false;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = PSN;

    if (qp->qp_type == IBV_QPT_UD) {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
            printf("failed to modify QP state to RTS\n");
            return false;
        }
    } else {
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE)) {
            printf("failed to modify QP state to RTS\n");
            return false;
        }
    }
    return true;
}