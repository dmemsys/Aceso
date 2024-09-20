#ifndef ACESO_RDMA_H
#define ACESO_RDMA_H

#include "KvUtils.h"
#include "Common.h"

#include <stdlib.h>
#include <vector>
#include <map>
#include <unordered_map>
#include <infiniband/verbs.h>

#define SIGNAL_BATCH 7
#define UD_PKEY 0x11111111
#define PSN 3185

constexpr int kMachineMax = 32;
constexpr int kWriteOroMax = 32;
constexpr int kReadOroMax = 32;

struct RdmaOpRegion {
  uint64_t local_addr;
  uint64_t remote_addr;
  uint32_t remote_sid;
  uint32_t size;
  // extended for send 
  uint32_t remote_tid;
  // extended for cas
  uint64_t compare_val;
  uint64_t swap_val;
  enum ibv_wr_opcode op_code;
};

struct RdmaContext {
  uint8_t port;
  int gid_index;

  ibv_context *ctx;
  ibv_pd *pd;

  uint16_t lid;
  union ibv_gid gid;

  RdmaContext() : ctx(NULL), pd(NULL) {}
};

int ib_connect_qp(struct ibv_qp * local_qp, 
    const struct QpInfo * local_qp_info, 
    const struct QpInfo * remote_qp_info, uint8_t conn_type, uint8_t role);
bool createContext(RdmaContext *context);
void *createMemoryRegion(uint64_t base_addr, uint64_t base_len);
bool modifyUDtoRTS(struct ibv_qp *qp, uint8_t port_num);

#endif