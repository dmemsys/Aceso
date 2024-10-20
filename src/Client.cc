#include "Client.h"

#include <assert.h>
#include <sys/mman.h>
#include <stdio.h>
#include <time.h>

#include <vector>
#include <fstream>
#include "Rdma.h"
#include "Common.h"

Client::Client(uint8_t server_id, uint8_t thread_id, uint32_t client_num, AddrCache * addr_cache, bool is_recovery, uint32_t cur_block_size) {
    remote_global_meta_addr = define::baseAddr;
    my_server_id        = server_id;
    my_thread_id        = thread_id;
    my_unique_id        = get_unique_id(server_id, thread_id);
    remote_meta_addr    = define::baseAddr + CLIENT_META_LEN * (my_unique_id - define::memoryNodeNum + 1);
    remote_root_addr    = define::baseAddr + META_AREA_BORDER;
    num_memory          = define::memoryNodeNum;
    last_allocated_mn   = my_unique_id;
    lock_coro_id        = 0xFF;

    block_size          = cur_block_size;
    subblock_num        = block_size / define::subblockSize;
    bmap_subblock_num   = (((subblock_num + 7) / 8 + 1023) / define::subblockSize);
    data_subblock_num   = subblock_num - bmap_subblock_num;
    alloc_watermark     = data_subblock_num / 2;

    assert(sizeof(RaceHashBucket) == 128);
    assert(sizeof(RaceHashSlot) == 16);

    if (define::replicaIndexNum == 3) {
        assert(define::cacheLevel == 0);
    }
    if (define::cacheLevel > 0) {
        assert(define::replicaIndexNum == 1);
    }

    this->addr_cache    = addr_cache;   assert(addr_cache != nullptr);

    kv_info_list    = NULL;
    kv_req_ctx_list = NULL;
    // statics initialize
    req_finish_num = 0, req_failed_num = 0, req_modify_num = 0, req_cas_num = 0;
    std::fill(req_search_num, req_search_num + 5, 0);
    cache_hit_num = 0, cache_mis_num = 0, cur_valid_kv_sz = 0, raw_valid_kv_sz = 0;
    for (int i = 0; i < 4; i++) {
        req_latency[i] = (uint64_t *)malloc(LATENCY_WINDOWS * sizeof(uint64_t));
        assert(req_latency[i] != NULL);
        memset(req_latency[i], 0, LATENCY_WINDOWS * sizeof(uint64_t));
    }

    createContext(&rdma_ctx);
    local_buf = mmap(NULL, define::clientBufferSize, PROT_READ | PROT_WRITE, 
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    assert(local_buf != MAP_FAILED);
    assert((uint64_t)local_buf % 64 == 0);
    local_buf_mr = ibv_reg_mr(rdma_ctx.pd, local_buf, define::clientBufferSize, 
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    assert(local_buf_mr != nullptr);

    race_root = (RaceHashRoot *)local_buf;
    coro_local_addr_list = (uint64_t *)malloc(sizeof(uint64_t) * define::maxCoroNum);
    for (int i = 0; i < define::maxCoroNum; i ++) {
        coro_local_addr_list[i] = (uint64_t)local_buf + define::coroBufferSize * (i + 1);
    }
    input_buf = (void *)((uint64_t)local_buf + (define::coroBufferSize * (define::maxCoroNum + 1)));
    degrade_buf = (void *)((uint64_t)input_buf + define::inputBufferSize);
    sim_gc_buf = (void *)((uint64_t)degrade_buf + define::degradeBufferSize);

    network_manager = new NetworkManager(CLIENT, &rdma_ctx, server_id, thread_id, client_num, local_buf_mr);
    network_manager->client_connect_rc_qps();
    
    _init_slab_map();
    if (define::codeMethod == RS_CODE) {
        _init_rs_code();
    }

    _get_race_root();
    if ((server_id == define::memoryNodeNum) && (thread_id == 0) && (!is_recovery)) {
        _init_block_for_load();
        _init_hash_table();
        network_manager->client_barrier("init-hash-" + std::to_string(is_recovery));
#ifdef ENABLE_ASYNC_INDEX
        _start_ckpt();
#endif
        network_manager->client_barrier("start-ckpt-" + std::to_string(is_recovery));
        _get_race_root();
    } else {
        _alloc_memory_block(nullptr, subblock_free_queue);
        network_manager->client_barrier("init-hash-" + std::to_string(is_recovery));
        network_manager->client_barrier("start-ckpt-" + std::to_string(is_recovery));
        _get_race_root();
    }
}

Client::~Client() {
    delete network_manager;
}

// INSERT, UPDATE, DELETE requests share the same procedure
int Client::kv_insert(KVReqCtx * ctx) {
    return kv_update(ctx);
}

int Client::kv_delete(KVReqCtx * ctx) {
    return kv_update(ctx);
}

int Client::kv_update(KVReqCtx * ctx) {
    _prepare_request(ctx);
    
    if (define::cacheLevel == 2) {
        _kv_update_read_cache(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
    }

    if (ctx->is_curr_kv_hit == false && ctx->is_prev_kv_hit == false) {
        _kv_update_read_buckets(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
        _kv_update_read_kv_from_buckets(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
    }
    
    _kv_update_write_kv_and_prepare_cas(ctx);
    if (ctx->is_finished) {
        goto update_exit;
    }
    _kv_update_cas_primary(ctx);

update_exit:
    _req_finish(ctx);
    return ctx->ret_val.ret_code;
}

int Client::kv_update_gc(KVReqCtx * ctx) {
    _prepare_request(ctx);
    ctx->is_sim_gc = true;     // turn on GC simulation

    if (define::cacheLevel == 2) {
        _kv_update_read_cache(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
    }

    if (ctx->is_curr_kv_hit == false && ctx->is_prev_kv_hit == false) {
        _kv_update_read_buckets(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
        _kv_update_read_kv_from_buckets(ctx);
        if (ctx->is_finished) {
            goto update_exit;
        }
    }

    _kv_update_write_kv_and_prepare_cas(ctx);
    if (ctx->is_finished) {
        goto update_exit;
    }
    _kv_update_cas_primary(ctx);

update_exit:
    _req_finish(ctx);
    ctx->is_sim_gc = false;
    return ctx->ret_val.ret_code;
}

void * Client::kv_search(KVReqCtx * ctx) {
    _prepare_request(ctx);
    
    if (define::cacheLevel == 2) {
        _kv_search_read_cache(ctx);
    }

    if (ctx->is_prev_kv_hit == false && ctx->is_curr_kv_hit == false) {
        _kv_search_read_buckets(ctx);
        _kv_search_read_kv_from_buckets(ctx);
        _kv_search_check_kv_from_buckets(ctx);
    }

    _req_finish(ctx);
    return ctx->ret_val.value_addr;
}

void * Client::kv_search_degrade(KVReqCtx * ctx) {
    _prepare_request(ctx);
    _kv_search_read_buckets_degrade(ctx);
    _kv_search_read_kv_degrade(ctx);
    _kv_search_check_kv_degrade(ctx);

    _req_finish(ctx);
    return ctx->ret_val.value_addr;
}

void Client::_init_slab_map() {
    assert(define::memoryNodeNum > 2);
    slab_num = define::memoryNodeNum;

    uint64_t memblock_area_total_len = round_down(define::serverMemorySize - HASH_AREA_BORDER, slab_num * block_size);
    uint64_t memblock_area_valid_len = memblock_area_total_len / slab_num * (slab_num - 2);
    uint64_t memblock_area_parit_len = memblock_area_total_len / slab_num * 2;
    
    slab_size  = memblock_area_total_len / slab_num;
    assert((slab_size % block_size) == 0);
    assert((slab_size & 0xFF) == 0);

    // first line of parity slabs
    for (int i = 0; i < slab_num; i ++) {
        uint64_t parity_block_off   = HASH_AREA_BORDER + (slab_num - 2) * slab_size;
        uint32_t st_server_id       = (i + 1) % slab_num;
        uint64_t st_block_off       = HASH_AREA_BORDER + (slab_num - 3) * slab_size;

        for(int k = 0; k < slab_num - 2; k++) {
            uint32_t tmp_server_id = (st_server_id + k) % slab_num;
            uint64_t tmp_block_off = st_block_off - k * slab_size;
            assert(tmp_block_off <= HASH_AREA_BORDER + (slab_num - 3) * slab_size);
            assert(tmp_block_off >= HASH_AREA_BORDER);

            parity_slab_map[parity_block_off | i].push_back(tmp_block_off | tmp_server_id);
            primar_slab_map[tmp_block_off | tmp_server_id].push_back(parity_block_off | i);
        }
    }

    // second line of parity slabs
    for (int i = 0; i < slab_num; i ++) {
        uint64_t parity_block_off   = HASH_AREA_BORDER + (slab_num - 1) * slab_size;
        uint32_t st_server_id       = (i + slab_num - 1) % slab_num;
        uint64_t st_block_off       = HASH_AREA_BORDER + (slab_num - 3) * slab_size;

        for(int k = 0; k < slab_num - 2; k++) {
            uint32_t tmp_server_id = (st_server_id + slab_num - k) % slab_num;
            uint64_t tmp_block_off = st_block_off - k * slab_size;
            assert(tmp_block_off <= HASH_AREA_BORDER + (slab_num - 3) * slab_size);
            assert(tmp_block_off >= HASH_AREA_BORDER);

            parity_slab_map[parity_block_off | i].push_back(tmp_block_off | tmp_server_id);
            primar_slab_map[tmp_block_off | tmp_server_id].push_back(parity_block_off | i);
        }
    }
}

void Client::_init_rs_code() {
    encode_matrix = (uint8_t *)malloc(32 * 32);         assert(encode_matrix != NULL);
    encode_table  = (uint8_t *)malloc(32 * 32 * 32);    assert(encode_table != NULL);
	gf_gen_cauchy1_matrix(encode_matrix, (define::codeK + define::codeP), define::codeK);
    ec_init_tables(define::codeK, define::codeP, &encode_matrix[define::codeK * define::codeK], encode_table);

    for (int i = 0; i < 3; i++) {
        decode_matrix[i] = (uint8_t *)malloc(32 * 32);      assert(decode_matrix[i] != NULL);
        invert_matrix[i] = (uint8_t *)malloc(32 * 32);      assert(invert_matrix[i] != NULL);
        decode_table[i]  = (uint8_t *)malloc(32 * 32 * 32); assert(decode_table[i] != NULL);
        memset(src_err_list[i], 0, 32);
        memset(src_in_err[i], 0, 32);
        src_err_list[i][0] = i;
        src_in_err[i][i] = 1;
        int ret = gf_gen_decode_matrix(encode_matrix, decode_matrix[i],
				    invert_matrix[i], decode_index[i], src_err_list[i], src_in_err[i],
				    1, 1, define::codeK, define::codeK + define::codeP);
        assert(ret == 0);
        ec_init_tables(define::codeK, 1, decode_matrix[i], decode_table[i]);
        ret = posix_memalign((void **)&temp_buff_list[i], 64, define::subblockSize); 
        assert(ret == 0);  
        memset(temp_buff_list[i], 0, define::subblockSize);
    }
}

void Client::_init_block_for_load(void) {
    std::deque<SubblockInfo> tmp_queue[define::memoryNodeNum];
    uint32_t loaderBlockNum = (16 * MB * define::memoryNodeNum) / block_size;

    for(int i = 0; i < loaderBlockNum; i++) {
        int queue_id = i % define::memoryNodeNum;
        _alloc_memory_block(nullptr, tmp_queue[queue_id]);
    }

    // interleave all subblocks
    for (int i = 0; i < loaderBlockNum * data_subblock_num; i++) {
        int queue_id = i % define::memoryNodeNum;
        SubblockInfo tmp_info = tmp_queue[queue_id].front();
        subblock_free_queue.push_back(tmp_info);
        tmp_queue[queue_id].pop_front();
    }
    for (int i = 0; i < define::memoryNodeNum; i ++) {
        assert(tmp_queue[i].size() == 0);
    }
}

void Client::_init_mmblock(ClientMMBlock * new_mm_block, MMBlockRegInfo * reg_info) {
    memset(new_mm_block , 0, sizeof(ClientMMBlock));
    memcpy(&new_mm_block->mr_info, &reg_info->mr_info, sizeof(struct MrInfo));
    memcpy(&new_mm_block->delta_mr_info_list[0], &reg_info->delta_mr_info_list[0], sizeof(struct MrInfo) * 2);
    memcpy(&new_mm_block->parity_mr_info_list[0], &reg_info->parity_mr_info_list[0], sizeof(struct MrInfo) * 2);
    new_mm_block->server_id = reg_info->server_id;
    new_mm_block->parity_server_id_list[0] = reg_info->parity_server_id_list[0];
    new_mm_block->parity_server_id_list[1] = reg_info->parity_server_id_list[1];
    new_mm_block->num_allocated = 0;
    new_mm_block->max_allocated = data_subblock_num;
    new_mm_block->inv_allocated = 0;
    new_mm_block->index_ver = 0;
}

void Client::_convert_addr_to_offset(uint64_t addr, uint64_t *offset, uint64_t *local_offset) {
    uint64_t total_offset = addr - define::baseAddr;
    assert(total_offset >= HASH_AREA_BORDER);
    *offset = HASH_AREA_BORDER + round_down(total_offset - HASH_AREA_BORDER, slab_size);
    *local_offset = (total_offset - HASH_AREA_BORDER) % slab_size;
}

void Client::_get_parity_slab_offsets(uint64_t offset, uint32_t server_id, 
    uint64_t parity_offsets[], uint32_t parity_server_ids[]) {
    assert((offset - HASH_AREA_BORDER) % slab_size == 0);
    auto it = primar_slab_map.find(offset | server_id);
    if (it == primar_slab_map.end()) {
        printf("Primary block offset wrong. offset: %lx, server_id: %x\n", offset, server_id);
        exit(1);
    }
    for (int i = 0; i < 2; i ++) {
        parity_server_ids[i] = (it->second[i] & 0xFF);
        assert(parity_server_ids[i] < MAX_MEM_NUM);
        parity_offsets[i]    = (it->second[i] >> 8) << 8;
        assert(parity_offsets[i] > 0);
        assert(((parity_offsets[i] - HASH_AREA_BORDER) % slab_size == 0) );
    }
}

void Client::_convert_offset_to_addr(uint64_t *addr, uint64_t offset, uint64_t local_offset) {
    *addr = define::baseAddr + offset + local_offset;
}

void Client::_get_kv_hash_info(KVInfo * kv_info, __OUT KVHashInfo * kv_hash_info) {
    uint64_t key_addr = (uint64_t)kv_info->l_addr + sizeof(KVLogHeader);
    kv_hash_info->hash_value = VariableLengthHash((void *)key_addr, kv_info->key_len, 0);
    kv_hash_info->prefix = (kv_hash_info->hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & RACE_HASH_MASK(race_root->global_depth);
    kv_hash_info->fp = HashIndexComputeFp(kv_hash_info->hash_value);
    kv_hash_info->local_depth = race_root->subtable_entry[kv_hash_info->prefix][0].local_depth;
}

void Client::_get_kv_addr_info(KVHashInfo * kv_hash_info, __OUT KVTableAddrInfo * kv_addr_info) {
    uint64_t hash_value = kv_hash_info->hash_value;
    uint64_t prefix     = kv_hash_info->prefix;

    uint64_t f_index_value = SubtableFirstIndex(hash_value, race_root->subtable_hash_range); 
    uint64_t s_index_value = SubtableSecondIndex(hash_value, f_index_value, race_root->subtable_hash_range);
    uint64_t f_idx, s_idx;

    if (f_index_value % 2 == 0) 
        f_idx = f_index_value / 2 * 3;
    else
        f_idx = f_index_value / 2 * 3 + 1;
    
    if (s_index_value % 2 == 0)
        s_idx = s_index_value / 2 * 3;
    else 
        s_idx = s_index_value / 2 * 3 + 1;
    
    // get combined bucket off
    kv_addr_info->f_main_idx = f_index_value % 2;
    kv_addr_info->s_main_idx = s_index_value % 2;
    kv_addr_info->f_idx = f_idx;
    kv_addr_info->s_idx = s_idx;

    for (int i = 0; i < define::replicaIndexNum; i ++) {
        uint64_t r_subtable_off;
        uint8_t target_server_id = race_root->subtable_entry[prefix][i].server_id;
        r_subtable_off = HashIndexConvert40To64Bits(race_root->subtable_entry[prefix][i].pointer);
        kv_addr_info->server_id[i] = target_server_id;
        kv_addr_info->f_bucket_addr[i]  = r_subtable_off + f_idx * sizeof(RaceHashBucket);
        kv_addr_info->s_bucket_addr[i]  = r_subtable_off + s_idx * sizeof(RaceHashBucket);
    }
}

void Client::_cache_search(KVReqCtx * ctx) {
    AddrCacheEntry * local_cache_entry = addr_cache->cache_search(ctx->key_str);
    if (local_cache_entry) {
        ctx->cache_entry = local_cache_entry;
        ctx->cache_slot = local_cache_entry->load_slot();
        ctx->cache_read_kv = local_cache_entry->read_kv;
    }
}

void Client::_fill_read_bucket_ror(RdmaOpRegion * ror, KVReqCtx * ctx) {
    ror[0].local_addr   = (uint64_t)ctx->local_bucket_addr;
    ror[0].remote_addr  = ctx->tbl_addr_info.f_bucket_addr[0];
    ror[0].remote_sid   = ctx->tbl_addr_info.server_id[0];
    ror[0].size         = 2 * sizeof(RaceHashBucket);

    ror[1].local_addr   = (uint64_t)ctx->local_bucket_addr + 2 * sizeof(RaceHashBucket);
    ror[1].remote_addr  = ctx->tbl_addr_info.s_bucket_addr[0];
    ror[1].remote_sid   = ctx->tbl_addr_info.server_id[0];
    ror[1].size         = 2 * sizeof(RaceHashBucket);
}

void Client::_fill_write_kv_ror(RdmaOpRegion * ror, KVInfo * a_kv_info, ClientMMAllocCtx * r_mm_info) {
    KVLogHeader * header = (KVLogHeader *)a_kv_info->l_addr;
    KVLogTail   * tail   = (KVLogTail *)((uint64_t)a_kv_info->l_addr + sizeof(KVLogHeader) + header->key_length + header->value_length);
    
    for (int i = 0; i < define::replicaNum; i++) {
        ror[i].local_addr  = (uint64_t)a_kv_info->l_addr;
        ror[i].remote_addr = r_mm_info->remote_addr[i];
        ror[i].remote_sid  = r_mm_info->server_id[i];
        if (header->op == KV_OP_DELETE)
            ror[i].size = sizeof(KVLogHeader) + sizeof(KVLogTail) + a_kv_info->key_len;
        else
            ror[i].size = define::subblockSize;
    }
}

void Client::_fill_read_kv_ror(RdmaOpRegion * ror, KVReqCtx * ctx, RaceHashSlot * local_slot) {    
    ror[0].remote_addr = ConvertOffsetToAddr(local_slot->atomic.offset);
    ror[0].remote_sid  = local_slot->atomic.server_id;
    ror[0].local_addr  = (uint64_t)ctx->local_kv_addr;
    ror[0].size        = define::subblockSize;
}

void Client::_fill_read_kv_ror(RdmaOpRegion * ror, const std::vector<KVRWAddr> & r_addr_list) {
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        ror[i].remote_addr = r_addr_list[i].r_kv_addr;
        ror[i].remote_sid  = r_addr_list[i].server_id;
        ror[i].local_addr  = r_addr_list[i].l_kv_addr;
        ror[i].size        = r_addr_list[i].length;
    }
}

void Client::_fill_cas_index_ror(RdmaOpRegion * ror, const KVCASAddr * cas_addr) {
    for (int i = 0; i < define::replicaIndexNum; i++) {
        ror[i].local_addr = cas_addr[i].l_kv_addr;
        ror[i].remote_addr = cas_addr[i].r_kv_addr;
        ror[i].remote_sid = cas_addr[i].server_id;
        ror[i].size = 8;
        ror[i].op_code = IBV_WR_ATOMIC_CMP_AND_SWP;
        ror[i].compare_val = cas_addr[i].orig_value;
        ror[i].swap_val = cas_addr[i].swap_value;
        this->req_cas_num++;
    }
}

void Client::_fill_read_index_ror(RdmaOpRegion * ror, const KVCASAddr & cas_addr) {
    ror[0].local_addr = cas_addr.l_kv_addr;
    ror[0].remote_addr = cas_addr.r_kv_addr;
    ror[0].remote_sid = cas_addr.server_id;
    ror[0].size = 8;
    ror[0].op_code = IBV_WR_RDMA_READ;
}

void Client::_fill_write_ver_ror(RdmaOpRegion * ror, std::vector<KVRWAddr> & ver_addr_list, void * local_addr, uint32_t length) {
    assert(ver_addr_list.size() == define::replicaNum);
    for (int i = 0; i < define::replicaNum; i++) {
        ror[i].local_addr = (uint64_t)local_addr;
        ror[i].remote_addr = ver_addr_list[i].r_kv_addr;
        ror[i].remote_sid = ver_addr_list[i].server_id;
        ror[i].size = length;
        ror[i].op_code = IBV_WR_RDMA_WRITE;
    }
}

void Client::_fill_slot(ClientMMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, RaceHashSlot * old_slot, __OUT RaceHashSlot * new_slot) {
    new_slot->atomic.fp = a_kv_hash_info->fp;
    new_slot->atomic.version = (old_slot->atomic.version + 1) % define::versionRound;
    new_slot->atomic.server_id = mm_alloc_ctx->server_id[0];
    new_slot->atomic.offset = ConvertAddrToOffset(mm_alloc_ctx->remote_addr[0]);
    
    new_slot->slot_meta.kv_len = 1;
    new_slot->slot_meta.epoch = old_slot->slot_meta.epoch;    // TODO: increment meta.epoch after every 256 increments to atomic.version
}

void Client::_fill_cas_addr(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot * old_slot, RaceHashSlot * new_slot) {
    KVCASAddr * cur_cas_addr;

    for (int i = 0; i < define::replicaIndexNum; i++) {
        cur_cas_addr = &ctx->kv_modify_pr_cas_addr[i];
        cur_cas_addr->r_kv_addr = remote_slot_addr[i];
        cur_cas_addr->l_kv_addr = (uint64_t)ctx->local_cas_return_value_addr + i * sizeof(uint64_t);
        cur_cas_addr->orig_value = ConvertSlotToUInt64(old_slot);
        cur_cas_addr->swap_value = ConvertSlotToUInt64(new_slot);
        cur_cas_addr->server_id  = ctx->tbl_addr_info.server_id[i];
    }

    ctx->new_slot = new_slot;

    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    header->slot_offset = ConvertAddrToOffset(remote_slot_addr[0]);
    header->slot_sid = ctx->tbl_addr_info.server_id[0];
}

void Client::_fill_read_cache_kv_ror(RdmaOpRegion * ror, RaceHashSlot * local_slot_val, uint64_t local_addr) {
    ror[0].remote_addr = ConvertSlotToAddr(local_slot_val);
    ror[0].remote_sid = local_slot_val->atomic.server_id;
    ror[0].local_addr = local_addr;
    ror[0].size = define::subblockSize;
    this->req_search_num[ror[0].remote_sid]++;
}

void Client::_fill_read_slot_ror(RdmaOpRegion * ror, uint32_t server_id, uint64_t r_slot_addr, uint64_t l_slot_addr) {
    ror[0].remote_addr = r_slot_addr;
    ror[0].remote_sid = server_id;
    ror[0].local_addr = l_slot_addr;
    ror[0].size = sizeof(RaceHashSlot);
}

int Client::_start_ckpt(void) {
    struct KVMsg * request;
    struct KVMsg * reply;

    request = _get_new_request(nullptr);
    request->type = REQ_CKPT_ENABLE;

    int ret = network_manager->send_and_recv_request(request, &reply, 0);
    if (reply->type != (request->type + REQ_NULL + 1)) {
        printf("error the alloc tmp type is not right, %d\n", reply->type);
    }
    return 0;
}

int Client::_alloc_from_sid(KVReqCtx * ctx, uint32_t server_id, int alloc_type, __OUT struct MrInfo * mr_info) {
    struct KVMsg * request;
    struct KVMsg * reply;

    request = _get_new_request(ctx);
    if (alloc_type == TYPE_KVBLOCK) {
        request->type = REQ_ALLOC;
        if (ctx != nullptr && ctx->is_sim_gc) {
            request->body.mr_info.addr = 0x1234;    // notifying remote server to simulate copying the origin DATA block
        } else {
            request->body.mr_info.addr = 0;
        }
    } else if (alloc_type == TYPE_SUBTABLE) {
        request->type = REQ_ALLOC_SUBTABLE;
    } else {
        printf("error in alloc_from_sid\n");
        return -1;
    }

    int ret = network_manager->send_and_recv_request(request, &reply, server_id, _get_coro_ctx(ctx));
    if (reply->type != (request->type + REQ_NULL + 1)) {
        printf("error the alloc type is not right, %d\n", reply->type);
    }
    memcpy(mr_info, &(reply->body.mr_info), sizeof(struct MrInfo));
    return 0;
}

int Client::_alloc_delta_from_sid(KVReqCtx * ctx, uint32_t server_id, int alloc_type, uint32_t primar_server_id, struct MrInfo * primar_mr_info, 
                                struct MrInfo * parity_mr_info, __OUT struct MrInfo * delta_mr_info) {
    struct KVMsg * request;
    struct KVMsg * reply;

    request = _get_new_request(ctx);
    request->type = REQ_ALLOC_DELTA;
    request->body.two_mr_info.prim_addr = primar_mr_info->addr;
    request->body.two_mr_info.prim_rkey = primar_mr_info->rkey;
    request->body.two_mr_info.pari_addr = parity_mr_info->addr;
    request->body.two_mr_info.pari_rkey = parity_mr_info->rkey;

    int ret = network_manager->send_and_recv_request(request, &reply, server_id, _get_coro_ctx(ctx));
    if (reply->type != (request->type + REQ_NULL + 1)) {
        printf("error the alloc tmp type is not right, %d\n", reply->type);
    }
    memcpy(delta_mr_info, &(reply->body.mr_info), sizeof(struct MrInfo));
    return 0;
}

int Client::_dyn_reg_new_subtable(uint32_t server_id, int alloc_type, __OUT struct MrInfo * mr_info) {
    int ret = 0;
    ClientMetaAddrInfo meta_info;
    
    assert(alloc_type == TYPE_SUBTABLE);
    meta_info.meta_info_type = TYPE_SUBTABLE;
    meta_info.server_id = server_id;
    meta_info.remote_addr = mr_info->addr;

    return 0;
}

void Client::_alloc_subtable(KVReqCtx * ctx, __OUT ClientMMAllocSubtableCtx * mm_ctx) {
    int ret = 0;
    for (int i = 0; i < define::replicaIndexNum; i ++) {
        struct MrInfo mr_info;
        uint32_t server_id = _get_alloc_mn_id();
        ret = _alloc_from_sid(ctx, server_id, TYPE_SUBTABLE, &mr_info);
        assert(ret == 0);
        mm_ctx[i].addr = mr_info.addr;
        mm_ctx[i].server_id = server_id;
    }
}

int Client::_init_hash_table() {
    // initialize remote subtable entry after getting root information
    for (int i = 0; i < RACE_HASH_INIT_SUBTABLE_NUM; i ++) {
        for (int j = 0; j < RACE_HASH_SUBTABLE_NUM / RACE_HASH_INIT_SUBTABLE_NUM; j ++) {
            uint64_t subtable_idx = j * RACE_HASH_INIT_SUBTABLE_NUM + i;
            ClientMMAllocSubtableCtx subtable_info[define::replicaIndexNum];
            _alloc_subtable(nullptr, subtable_info);
            for (int r = 0; r < define::replicaIndexNum; r ++) {
                race_root->subtable_entry[subtable_idx][r].lock        = 0;
                race_root->subtable_entry[subtable_idx][r].local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
                race_root->subtable_entry[subtable_idx][r].server_id   = subtable_info[r].server_id;
                assert((subtable_info[r].addr & 0xFF) == 0);
                HashIndexConvert64To40Bits(subtable_info[r].addr, race_root->subtable_entry[subtable_idx][r].pointer);
            }
        }
    }

    // write root information back to all replicas
    int ret = _write_race_root();
    assert(ret == 0);

    return 0;
}

void Client::_prepare_request(KVReqCtx * ctx) {
    ctx->is_finished        = false;
    ctx->is_prev_kv_hit     = false;
    ctx->is_curr_kv_hit     = false;
    ctx->has_allocated      = false;
    ctx->has_finished       = false;
    ctx->has_previous       = false;
    ctx->is_sim_gc          = false;
    ctx->cache_entry        = nullptr;

    _get_kv_hash_info(ctx->kv_info, &ctx->hash_info);
    _get_kv_addr_info(&ctx->hash_info, &ctx->tbl_addr_info);
}

void Client::_find_kv_in_buckets(KVReqCtx * ctx) {
    _get_local_bucket_info(ctx);
    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    ctx->kv_read_addr_list.clear();
    ctx->kv_idx_list.clear();

    // search all kv pair that finger print matches
    for (int i = 0; i < 4; i ++) {
        assert(ctx->bucket_arr[i]->local_depth == ctx->hash_info.local_depth);
        for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
            if (ctx->slot_arr[i][j].atomic.fp == ctx->hash_info.fp &&
                ctx->slot_arr[i][j].atomic.offset != 0) {
                // push the offset to the lists
                KVRWAddr cur_kv_addr;
                cur_kv_addr.server_id = ctx->slot_arr[i][j].atomic.server_id;
                cur_kv_addr.r_kv_addr = ConvertOffsetToAddr(ctx->slot_arr[i][j].atomic.offset);
                cur_kv_addr.l_kv_addr = local_kv_buf_addr;
                cur_kv_addr.length    = define::subblockSize;

                ctx->kv_read_addr_list.push_back(cur_kv_addr);
                ctx->kv_idx_list.push_back(std::make_pair(i, j));
                local_kv_buf_addr += cur_kv_addr.length;
            }
        }
    }
}

void Client::_find_empty_slot(KVReqCtx * ctx) {
    _get_local_bucket_info(ctx);

    uint32_t f_main_idx = ctx->tbl_addr_info.f_main_idx;
    uint32_t s_main_idx = ctx->tbl_addr_info.s_main_idx;
    uint32_t f_free_num, s_free_num;
    uint32_t f_free_slot_idx, s_free_slot_idx;
    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;
    uint32_t f_free_slot_idx_list[RACE_HASH_ASSOC_NUM];
    uint32_t s_free_slot_idx_list[RACE_HASH_ASSOC_NUM];
    std::vector<std::pair<uint32_t, uint32_t>> empty_idx_pair_vec;
    for (int i = 0; i < 2; i ++) {
        f_free_num = GetFreeSlotNum(ctx->f_com_bucket + f_main_idx, &f_free_slot_idx);
        s_free_num = GetFreeSlotNum(ctx->s_com_bucket + s_main_idx, &s_free_slot_idx);

        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                bucket_idx = f_main_idx;
                slot_idx = f_free_slot_idx;
            } else {
                bucket_idx = 2 + s_main_idx;
                slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }

    ctx->bucket_idx = bucket_idx;
    ctx->slot_idx   = slot_idx;
}

int32_t Client::_find_match_slot(KVReqCtx * ctx) {
    int32_t ret = 0;

    for (size_t i = 0; i < ctx->kv_read_addr_list.size(); i ++) {
        uint64_t read_key_addr = ctx->kv_read_addr_list[i].l_kv_addr + sizeof(KVLogHeader);
        uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
        KVLogHeader * header = (KVLogHeader *)ctx->kv_read_addr_list[i].l_kv_addr;
        if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
            ctx->prev_ver = header->version.val;
            ctx->match_idx = i;
            return i;
        }
    }
    ctx->match_idx = -1;
    return -1;
}

void Client::_prepare_old_slot(KVReqCtx * ctx, uint64_t * remote_slot_addr, RaceHashSlot ** old_slot, RaceHashSlot ** new_slot) {
    assert(remote_slot_addr != nullptr);
    assert(old_slot != nullptr);
    *new_slot = (RaceHashSlot *)ctx->local_cas_target_value_addr;
    if (!ctx->has_previous) {
        if (ctx->bucket_idx < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            uint64_t old_local_slot_addr = (uint64_t)&(ctx->f_com_bucket[ctx->bucket_idx].slots[ctx->slot_idx]);
            for (int i = 0; i < define::replicaIndexNum; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            *old_slot = (RaceHashSlot *)old_local_slot_addr;
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            uint64_t old_local_slot_addr = (uint64_t)&(ctx->s_com_bucket[ctx->bucket_idx - 2].slots[ctx->slot_idx]);
            for (int i = 0; i < define::replicaIndexNum; i ++) {
                remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
            }
            *old_slot = (RaceHashSlot *)old_local_slot_addr;
        } 
    } else {
        if (ctx->is_prev_kv_hit) {
            for (int i = 0; i < define::replicaIndexNum; i ++) {
                remote_slot_addr[i] = ctx->cache_entry->r_slot_addr;
            }
            *old_slot = &ctx->cache_slot;
        } else if (ctx->is_curr_kv_hit) {
            for (int i = 0; i < define::replicaIndexNum; i ++) {
                remote_slot_addr[i] = ctx->cache_entry->r_slot_addr;
            }
            *old_slot = (RaceHashSlot *)ctx->local_slot_addr;
        } else {
            std::pair<int32_t, int32_t> idx_pair = ctx->kv_idx_list[ctx->match_idx];
            int32_t bucket_idx = idx_pair.first;
            int32_t slot_idx   = idx_pair.second;
            if (bucket_idx < 2) {
                uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
                uint64_t old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_idx].slots[slot_idx]);
                for (int i = 0; i < define::replicaIndexNum; i ++) {
                    remote_slot_addr[i] = ctx->tbl_addr_info.f_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
                }
                *old_slot = (RaceHashSlot *)old_local_slot_addr;
            } else {
                uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
                uint64_t old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_idx - 2].slots[slot_idx]);
                for (int i = 0; i < define::replicaIndexNum; i ++) {
                    remote_slot_addr[i] = ctx->tbl_addr_info.s_bucket_addr[i] + (old_local_slot_addr - local_com_bucket_addr);
                }
                *old_slot = (RaceHashSlot *)old_local_slot_addr;
            }
        }
    }
}

void Client::_get_local_bucket_info(KVReqCtx * ctx) {
    ctx->f_com_bucket = ctx->local_bucket_addr;
    ctx->s_com_bucket = ctx->local_bucket_addr + 2;
    assert((uint64_t)ctx->s_com_bucket - (uint64_t)ctx->f_com_bucket == 2 * sizeof(RaceHashBucket));

    ctx->bucket_arr[0] = ctx->f_com_bucket;
    ctx->bucket_arr[1] = ctx->f_com_bucket + 1;
    ctx->bucket_arr[2] = ctx->s_com_bucket;
    ctx->bucket_arr[3] = ctx->s_com_bucket + 1;

    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
    ctx->slot_arr[2] = ctx->s_com_bucket[0].slots;
    ctx->slot_arr[3] = ctx->s_com_bucket[1].slots;

    // check if the depth matches
    assert((ctx->f_com_bucket->local_depth) == (ctx->hash_info.local_depth));
    assert(ctx->s_com_bucket->local_depth == ctx->hash_info.local_depth);
}

void Client::_prepare_ver_addr(KVReqCtx * ctx) {
    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    uint32_t tail_offset = offsetof(KVLogHeader, version); 
    ctx->kv_ver_addr_list.clear();
    for (int i = 0; i < define::replicaNum; i ++) {
        KVRWAddr cur_rw_addr;
        cur_rw_addr.l_kv_addr = (uint64_t)ctx->local_kv_addr;
        cur_rw_addr.r_kv_addr = ctx->mm_alloc_ctx.remote_addr[i] + tail_offset;
        cur_rw_addr.server_id = ctx->mm_alloc_ctx.server_id[i];
        cur_rw_addr.length = sizeof(header->version);
        ctx->kv_ver_addr_list.push_back(cur_rw_addr);
    }
}

// get the primary block's index in xor map
uint32_t Client::_get_index_in_xor_map(uint64_t pr_addr) {
    uint64_t pr_off = pr_addr - define::baseAddr;
    uint16_t ret = (slab_num - 3) - ((pr_off - HASH_AREA_BORDER) / slab_size);
    assert(ret < (slab_num - 2));
    return ret;
}

CoroContext * Client::_get_coro_ctx(KVReqCtx * ctx) {
    if (ctx == nullptr)
        return nullptr;
    else
        return ctx->coro_ctx;
}

int Client::_write_new_block_meta(CoroContext *ctx, MMBlockRegInfo * reg_info) {
    RdmaOpRegion ror_list[define::memoryNodeNum];
    MMBlockMeta prim_meta;
    prim_meta.block_role = DATA_BLOCK;
    prim_meta.is_broken = 0;
    prim_meta.normal.index_ver = define::unXorSeal;
    prim_meta.normal.xor_idx = _get_index_in_xor_map(reg_info->mr_info.addr);
    
    for (int i = 0; i < define::memoryNodeNum; i++) {
        ror_list[i].local_addr = (uint64_t)&prim_meta;
        ror_list[i].remote_addr = get_meta_addr(reg_info->mr_info.addr, reg_info->server_id, block_size);
        ror_list[i].remote_sid = i;
        ror_list[i].size = sizeof(MMBlockMeta);
    }
    network_manager->rdma_write_batches_sync(ror_list, define::memoryNodeNum, ctx);
}

int Client::_write_seal_block_meta(CoroContext *ctx, uint64_t block_addr, uint32_t server_id, uint64_t index_ver) {
    RdmaOpRegion ror_list[define::memoryNodeNum];
    MMBlockMeta prim_meta;
    prim_meta.block_role = DATA_BLOCK;
    prim_meta.is_broken = 0;
    prim_meta.normal.index_ver = index_ver;
    prim_meta.normal.xor_idx = _get_index_in_xor_map(block_addr);
    
    for (int i = 0; i < define::memoryNodeNum; i++) {
        ror_list[i].local_addr = (uint64_t)&prim_meta;
        ror_list[i].remote_addr = get_meta_addr(block_addr, server_id, block_size);
        ror_list[i].remote_sid = i;
        ror_list[i].size = sizeof(MMBlockMeta);
    }

    network_manager->rdma_write_batches_sync(ror_list, define::memoryNodeNum, ctx);
}

int Client::_xor_mem_block(KVReqCtx * ctx, ClientMMBlock * mm_block) {
    int ret = 0;
    struct KVMsg * request[3], * reply[3];

    this->raw_valid_kv_sz -= 2 * data_subblock_num * define::subblockSize;

    uint64_t index_ver = define::unXorSeal;
    for(int i = 0; i < 3; i++) {
        request[i] = _get_new_request(ctx);
        if (i == 0) {
            request[i]->type = REQ_SEAL;
            request[i]->body.mr_info.addr = mm_block->mr_info.addr;
            network_manager->send_and_recv_request(request[i], &reply[i], mm_block->server_id, ctx->coro_ctx);
            if (reply[i]->type != REP_SEAL) {
                printf("error the type is not seal, %d\n", reply[i]->type);
            }
            assert(reply[i]->type == REP_SEAL);
        } else {
            request[i]->type = REQ_XOR;
            request[i]->body.xor_info.xor_size = block_size;
            request[i]->body.xor_info.parity_addr   = mm_block->parity_mr_info_list[i - 1].addr;
            request[i]->body.xor_info.delta_addr    = mm_block->delta_mr_info_list[i - 1].addr;
            network_manager->send_and_recv_request(request[i], &reply[i], mm_block->parity_server_id_list[i - 1], ctx->coro_ctx);
            if (reply[i]->type != REP_XOR) {
                printf("error the type is not xor, %d\n", reply[i]->type);
            }
            assert(reply[i]->type == REP_XOR);
            mm_block->index_ver = reply[i]->body.xor_info.index_ver;
            if (index_ver == define::unXorSeal)
                index_ver = reply[i]->body.xor_info.index_ver;
        }
    }

    _write_seal_block_meta(ctx->coro_ctx, mm_block->mr_info.addr, mm_block->server_id, index_ver);
    return 0;
}

struct KVMsg * Client::_get_new_request(KVReqCtx * ctx) {
    struct KVMsg * request = (struct KVMsg *)network_manager->rpc_get_send_pool();
    request->nd_id = my_server_id;
    request->th_id = my_thread_id;
    if (ctx == nullptr || ctx->coro_ctx == nullptr)
        request->co_id = 0;
    else
        request->co_id = ctx->coro_ctx->coro_id;
    return request;
}

void Client::_req_finish(KVReqCtx * ctx) {
    assert(ctx->is_finished == true);

    if (ctx->req_type == KV_OP_SEARCH && ctx->ret_val.value_addr != NULL && ctx->read_header->op == KV_OP_DELETE)
        ctx->ret_val.value_addr = NULL;

    if (ctx->has_allocated == false)
        return;
    if (ctx->has_finished == true)
        return;

    // merge memblock
    ClientMMBlock * mm_block = (ClientMMBlock *)(ctx->mm_alloc_ctx.local_mmblock_addr);
    assert(mm_block != NULL);
    
    if(mm_block->num_allocated.fetch_add(1) == (mm_block->max_allocated - 1)) {
        assert(mm_block->num_allocated == mm_block->max_allocated);
        _xor_mem_block(ctx, mm_block);
    }
    assert(mm_block->num_allocated <= mm_block->max_allocated);
    
    ctx->has_finished = true;
}

void Client::_alloc_new_kv(KVReqCtx * ctx) {
    int ret = 0;
    KVLogHeader * head = (KVLogHeader *)ctx->kv_info->l_addr;
    if (ctx->req_type == KV_OP_DELETE)
        head->value_length = 0;
    KVLogTail * tail = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader) + head->key_length + head->value_length);
    head->op = ctx->req_type;
    head->write_version = 1; // 2-bit: 01
    tail->write_version = 1;
    head->version.val = ctx->prev_ver + 1;

    // allocate remote memory
    uint32_t kv_block_size = head->key_length + head->value_length + sizeof(KVLogHeader) + sizeof(KVLogTail);
    _mm_alloc(ctx, kv_block_size, &ctx->mm_alloc_ctx);
    return;
}

void Client::_write_new_kv(KVReqCtx * ctx) {
    // prepare version write addr
    _prepare_ver_addr(ctx);

    // generate send requests
    RdmaOpRegion ror_list[define::replicaNum];
    _fill_write_kv_ror(ror_list, ctx->kv_info, &ctx->mm_alloc_ctx);
    network_manager->rdma_write_batches_sync(ror_list, define::replicaNum, ctx->coro_ctx);
    ctx->is_finished = false;
    return;
}

void Client::_modify_primary_idx(KVReqCtx * ctx) {
    int ret = 0;
    // update primary index
    RdmaOpRegion ror_list[define::replicaIndexNum];
    _fill_cas_index_ror(ror_list, ctx->kv_modify_pr_cas_addr);
    network_manager->rdma_mix_batches_sync(ror_list, define::replicaIndexNum, ctx->coro_ctx);

    // cas primary failed
    if (*(uint64_t *)ctx->kv_modify_pr_cas_addr[0].l_kv_addr != ctx->kv_modify_pr_cas_addr[0].orig_value) {
        if (ctx->req_type == KV_OP_INSERT) {
            ctx->ret_val.ret_code = KV_OPS_FAIL_REDO;
            ctx->is_finished = true;
        } else if (ctx->req_type == KV_OP_UPDATE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS; 
            ctx->is_finished = true;
        } else if (ctx->req_type == KV_OP_DELETE) {
            ctx->ret_val.ret_code = KV_OPS_SUCCESS;
            ctx->is_finished = true;
        }
        // write failed version number
        uint64_t ver_num = define::invalidVersion;
        RdmaOpRegion ror_list[define::replicaNum];
        _fill_write_ver_ror(ror_list, ctx->kv_ver_addr_list, &ver_num, sizeof(uint64_t));
        network_manager->rdma_write_batches_sync(ror_list, define::replicaNum, ctx->coro_ctx);
        return;
    }

    ctx->is_finished = true;
    ctx->ret_val.ret_code = KV_OPS_SUCCESS;

    if (ctx->has_previous)
        _mm_free(ctx->kv_modify_pr_cas_addr[0].orig_value);

    if (define::cacheLevel >= 1) {
        uint64_t r_slot_addr = ctx->kv_modify_pr_cas_addr[0].r_kv_addr;
        uint64_t local_cas_val = ctx->kv_modify_pr_cas_addr[0].swap_value;
        addr_cache->cache_update(ctx->key_str, ctx->tbl_addr_info.server_id[0], ctx->new_slot, r_slot_addr);
    }

    return;
}

void Client::_mm_free_cur(const ClientMMAllocCtx * ctx) {
    SubblockInfo last_allocated;
    
    for (int i = 0; i < define::replicaNum; i++) {
        last_allocated.remote_addr[i] = ctx->remote_addr[i];
        last_allocated.server_id[i] = ctx->server_id[i];
        last_allocated.local_mmblock_addr = ctx->local_mmblock_addr;
    }

    subblock_free_queue.push_front(last_allocated);

    SubblockInfo last_last_allocated;
    for (int i = 0; i < define::replicaNum; i++) {
        last_last_allocated.remote_addr[i] = ctx->prev_addr[i];
        last_last_allocated.server_id[i] = ctx->prev_server_id[i];
        last_last_allocated.local_mmblock_addr = ctx->prev_mmblock_addr;
    }
    last_allocated_info = last_last_allocated;
}

void Client::_mm_free(uint64_t old_slot) {
    RaceHashAtomic slot = *(RaceHashAtomic *)&old_slot;
    uint64_t kv_addr = ConvertOffsetToAddr(slot.offset);
    uint32_t subblock_id = (kv_addr % define::memoryBlockSize) / define::subblockSize;
    uint64_t block_addr = kv_addr - (kv_addr % define::memoryBlockSize);
    uint32_t subblock_8byte_offset = subblock_id / 64;
    
    uint64_t bmap_addr = block_addr + subblock_8byte_offset * sizeof(uint64_t);
    uint64_t add_value = 1 << (subblock_id % 64);

    char tmp[256] = {0};
    sprintf(tmp, "%lu@%d", bmap_addr, slot.server_id);
    std::string addr_str(tmp);
    free_bit_map[addr_str] += add_value;
}

void Client::_mm_alloc(KVReqCtx * ctx, size_t size, __OUT ClientMMAllocCtx * mm_ctx) {
    int ret = 0;

    assert(size <= define::subblockSize);
    assert(subblock_free_queue.size() > 0);
    SubblockInfo alloc_subblock = subblock_free_queue.front();
    subblock_free_queue.pop_front();
    ctx->has_allocated = true;

    this->raw_valid_kv_sz += define::subblockSize * 3;
    this->cur_valid_kv_sz += define::subblockSize;

    // must ensure that next_subblock not null
    if (subblock_free_queue.size() < alloc_watermark && lock_coro_id == 0xFF) {
        lock_coro_id = ctx->coro_id;
        _alloc_memory_block(ctx, subblock_free_queue);
        lock_coro_id = 0xFF;
    }

    // here we simulate xoring the cur_kv with old_kv for GC
    if (ctx != nullptr && ctx->is_sim_gc) {
        _sim_gc_xor_kv(ctx, alloc_subblock);
    }

    SubblockInfo next_subblock = subblock_free_queue.front();
    
    mm_ctx->local_mmblock_addr = alloc_subblock.local_mmblock_addr; assert(alloc_subblock.local_mmblock_addr != 0);
    mm_ctx->next_mmblock_addr = next_subblock.local_mmblock_addr;
    mm_ctx->prev_mmblock_addr = last_allocated_info.local_mmblock_addr;
    for (int i = 0; i < define::replicaNum; i++) {
        mm_ctx->remote_addr[i]    = alloc_subblock.remote_addr[i];
        mm_ctx->server_id[i]      = alloc_subblock.server_id[i];

        mm_ctx->next_addr[i]      = next_subblock.remote_addr[i];
        mm_ctx->next_server_id[i] = next_subblock.server_id[i];
        
        mm_ctx->prev_addr[i]      = last_allocated_info.remote_addr[i];
        mm_ctx->prev_server_id[i] = last_allocated_info.server_id[i];
    }

    last_allocated_info = alloc_subblock;
}

void Client::_alloc_memory_block(KVReqCtx * ctx, std::deque<SubblockInfo> &free_queue) {
    int ret = 0;
    uint32_t primar_server_id = _get_alloc_mn_id();

    MMBlockRegInfo reg_info;
    reg_info.server_id = primar_server_id;
    ret = _alloc_from_sid(ctx, primar_server_id, TYPE_KVBLOCK, &reg_info.mr_info);
    assert(ret == 0);
    assert(reg_info.mr_info.addr != 0);
    assert((reg_info.mr_info.addr & 0xFF) == 0);
    uint64_t primar_addr = reg_info.mr_info.addr;
    uint64_t primar_offset, local_offset;
    uint64_t parity_offsets[2], parity_addrs[2];
    uint32_t parity_server_ids[2];

    // here we simulate fetching the old block for GC
    if (ctx != nullptr && ctx->is_sim_gc) {
        _sim_gc_fetch_block(ctx, reg_info.mr_info.addr, primar_server_id);
    }

    _convert_addr_to_offset(primar_addr, &primar_offset, &local_offset);
    _get_parity_slab_offsets(primar_offset, primar_server_id, parity_offsets, parity_server_ids);
    _convert_offset_to_addr(&parity_addrs[0], parity_offsets[0], local_offset);
    _convert_offset_to_addr(&parity_addrs[1], parity_offsets[1], local_offset);

    // alloc two delta blocks
    for(int i = 0; i < 2; i++) {
        reg_info.parity_mr_info_list[i].addr = parity_addrs[i];
        reg_info.parity_server_id_list[i] = parity_server_ids[i];

        ret = _alloc_delta_from_sid(ctx, parity_server_ids[i], TYPE_DELTA_KVBLOCK, reg_info.server_id, &reg_info.mr_info, &reg_info.parity_mr_info_list[i], &reg_info.delta_mr_info_list[i]);
        assert(ret == 0);
        assert(reg_info.delta_mr_info_list[i].addr != 0);
        assert((reg_info.delta_mr_info_list[i].addr & 0xFF) == 0);
    }

    ret = _dyn_reg_new_block(ctx, &reg_info, TYPE_KVBLOCK, free_queue);

    assert(ret == 0);
}

int Client::_dyn_reg_new_block(KVReqCtx * ctx, MMBlockRegInfo * reg_info, int alloc_type, std::deque<SubblockInfo> &free_queue) {
    assert(alloc_type == TYPE_KVBLOCK);
    // send meta info to remote
    _write_new_block_meta(_get_coro_ctx(ctx), reg_info);

    ClientMMBlock * new_mm_block = (ClientMMBlock *)malloc(sizeof(ClientMMBlock));
    _init_mmblock(new_mm_block, reg_info);

    // add subblocks to subblock list
    for (int i = bmap_subblock_num; i < subblock_num; i ++) {
        SubblockInfo tmp_info;
        for(int j = 0; j < 3; j++) {
            if (j == 0) {
                tmp_info.remote_addr[j] = new_mm_block->mr_info.addr + i * define::subblockSize;
                assert((tmp_info.remote_addr[j] & 0xFF) == 0);
                tmp_info.server_id[j] = new_mm_block->server_id;
            }
            else {
                tmp_info.remote_addr[j] = new_mm_block->delta_mr_info_list[j - 1].addr + i * define::subblockSize;
                assert((tmp_info.remote_addr[j] & 0xFF) == 0);
                tmp_info.server_id[j] = new_mm_block->parity_server_id_list[j - 1];
            }
        }

        tmp_info.local_mmblock_addr = (uint64_t)new_mm_block;
        assert(tmp_info.local_mmblock_addr != 0);
        free_queue.push_back(tmp_info);
    }
    mm_blocks.push_back(new_mm_block);

    return 0;
}

void Client::_kv_search_read_cache(KVReqCtx * ctx) {
    _cache_read_prev_kv(ctx);
    if (ctx->is_prev_kv_hit) {
        ctx->read_header = (KVLogHeader *)ctx->local_cache_addr;
        ctx->ret_val.value_addr = (void *)((uint64_t)ctx->read_header + sizeof(KVLogHeader) + ctx->read_header->key_length);
        ctx->is_finished = true;
        return;
    }
    _cache_read_curr_kv(ctx);
    if (ctx->is_curr_kv_hit) {
        uint64_t read_key_addr = (uint64_t)ctx->local_kv_addr + sizeof(KVLogHeader);
        ctx->read_header = (KVLogHeader *)ctx->local_kv_addr;
        ctx->ret_val.value_addr = (void *)(read_key_addr + ctx->read_header->key_length);
        ctx->is_finished = true;
        return;
    }
}

void Client::_kv_search_read_buckets(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    int k = 2;
    RdmaOpRegion ror_list[3];
    _fill_read_bucket_ror(ror_list, ctx); 

    if (define::cacheLevel == 1) {
        _cache_search(ctx);
        if (ctx->cache_entry && ctx->cache_read_kv) {
            _fill_read_cache_kv_ror(&ror_list[2], (RaceHashSlot *)(&ctx->cache_slot), (uint64_t)ctx->local_cache_addr);
            k += 1;
        }
    }

    network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);
    
    ctx->is_finished = false;
    return;
}

void Client::_kv_search_read_buckets_degrade(KVReqCtx * ctx) {
    int ret = 0;

    RdmaOpRegion ror_list[2];
    _fill_read_bucket_ror(ror_list, ctx); 

    network_manager->rdma_read_batches_sync(ror_list, 2, ctx->coro_ctx);

    ctx->is_finished = false;
    return;
}

void Client::_kv_search_read_kv_from_buckets(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    if ((define::cacheLevel == 1) && ctx->cache_entry && ctx->cache_read_kv) {
        ctx->read_header = (KVLogHeader *)ctx->local_cache_addr;
        if (_cache_entry_is_valid(ctx)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            if (CheckKey((void *)read_key_addr, ctx->read_header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                cache_hit_num++;
                ctx->cache_entry->acc_cnt++; // update cache counter
                ctx->is_prev_kv_hit = true;
                ctx->ret_val.value_addr = (void *)((uint64_t)ctx->read_header + sizeof(KVLogHeader) + ctx->read_header->key_length);
                ctx->is_finished = true;
                return;
            }
        }
        cache_mis_num++;
        ctx->cache_entry->miss_cnt++;
    }

    _find_kv_in_buckets(ctx);
    if (ctx->kv_read_addr_list.size() == 0) {
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }

    int k = ctx->kv_read_addr_list.size();
    RdmaOpRegion * ror_list = new RdmaOpRegion[k];
    _fill_read_kv_ror(ror_list, ctx->kv_read_addr_list);
    network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);
    
    delete[] ror_list;
    ctx->is_finished = false;
    return;
}

void Client::_kv_search_read_kv_degrade(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    _find_kv_in_buckets_degrade(ctx);
    if (ctx->kv_recover_addr_list.size() == 0) {
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }

    int k = 0;
    for (int i = 0; i < ctx->kv_recover_addr_list.size(); i++) {
        k += ctx->kv_recover_addr_list[i].local_addr_count;
    }
    RdmaOpRegion * ror_list = new RdmaOpRegion[k];
    _fill_recover_kv_ror(ror_list, ctx->kv_recover_addr_list);
    network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);
    _xor_all_kv(ctx->kv_recover_addr_list);

    delete[] ror_list;
    ctx->is_finished = false;
    return;
}

void Client::_kv_search_check_kv_from_buckets(KVReqCtx * ctx) {
    int ret = 0;

    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }
    
    int32_t match_idx = _find_match_slot(ctx);
    if (match_idx != -1) {
        uint64_t read_key_addr = ctx->kv_read_addr_list[match_idx].l_kv_addr + sizeof(KVLogHeader);
        ctx->read_header = (KVLogHeader *)ctx->kv_read_addr_list[match_idx].l_kv_addr;
        
        int bucket_id = ctx->kv_idx_list[match_idx].first;
        int slot_id   = ctx->kv_idx_list[match_idx].second;
        uint64_t remote_slot_addr;
        uint64_t old_local_slot_addr;
        _get_local_bucket_info(ctx);
        if (bucket_id < 2) {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->f_com_bucket;
            old_local_slot_addr   = (uint64_t)&(ctx->f_com_bucket[bucket_id].slots[slot_id]);
            remote_slot_addr = ctx->tbl_addr_info.f_bucket_addr[0] + (old_local_slot_addr - local_com_bucket_addr);
        } else {
            uint64_t local_com_bucket_addr = (uint64_t)ctx->s_com_bucket;
            old_local_slot_addr   = (uint64_t)&(ctx->s_com_bucket[bucket_id - 2].slots[slot_id]);
            remote_slot_addr = ctx->tbl_addr_info.s_bucket_addr[0] + (old_local_slot_addr - local_com_bucket_addr);
        }

        ctx->ret_val.value_addr = (void *)(read_key_addr + ctx->read_header->key_length);
        ctx->is_finished = true;

        if (define::cacheLevel >= 1) {
            addr_cache->cache_update(ctx->key_str, ctx->tbl_addr_info.server_id[0], (RaceHashSlot *)old_local_slot_addr, remote_slot_addr);
        }
        return;
    } else {
        printf("no match! addr list size %lu\n", ctx->kv_read_addr_list.size());
        printf("fp %d sid %d\n", ctx->hash_info.fp, ctx->kv_read_addr_list[0].server_id);
        printf("bid %llu\n", (ctx->kv_read_addr_list[0].r_kv_addr - define::baseAddr - HASH_AREA_BORDER) / block_size);
        char * read_key_addr = (char *)ctx->kv_read_addr_list[0].l_kv_addr + sizeof(KVLogHeader);
        printf("read key %s\n", std::string(read_key_addr, 16).c_str());
        printf("cur key %s\n", ctx->key_str.c_str());
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }
}

void Client::_kv_search_check_kv_degrade(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }
    
    int32_t match_idx = _find_match_kv_idx_degrade(ctx);
    if (match_idx != -1) {
        uint64_t read_key_addr = ctx->kv_recover_addr_list[match_idx].l_kv_addr + sizeof(KVLogHeader);
        ctx->read_header = (KVLogHeader *)ctx->kv_recover_addr_list[match_idx].l_kv_addr;
        ctx->ret_val.value_addr = (void *)(read_key_addr + ctx->read_header->key_length);
        ctx->is_finished = true;
        return;
    } else {
        printf("no match! addr list size %lu\n", ctx->kv_recover_addr_list.size());
        printf("fp %d\n", ctx->hash_info.fp);
        char * read_key_addr = (char *)ctx->kv_recover_addr_list[0].l_kv_addr + sizeof(KVLogHeader);
        printf("read key %s\n", std::string(read_key_addr, 16).c_str());
        printf("cur key %s\n", ctx->key_str.c_str());
        ctx->ret_val.value_addr = NULL;
        ctx->is_finished = true;
        return;
    }
}

void Client::_kv_update_read_cache(KVReqCtx * ctx) {
    _cache_read_prev_kv(ctx);
    if (ctx->is_prev_kv_hit) {
        KVLogHeader * header = (KVLogHeader *)ctx->local_cache_addr;
        ctx->prev_ver = header->version.val;
        return;
    }
    _cache_read_curr_kv(ctx);
    if (ctx->is_curr_kv_hit) {
        KVLogHeader * header = (KVLogHeader *)ctx->local_kv_addr;
        ctx->prev_ver = header->version.val;
        return;
    }
}

void Client::_kv_update_read_buckets(KVReqCtx * ctx) {
    int ret = 0;
    int k = 2;
    RdmaOpRegion ror_list[3];
    _fill_read_bucket_ror(ror_list, ctx); 

    if (define::cacheLevel == 1) {
        _cache_search(ctx);
        if (ctx->cache_entry && ctx->cache_read_kv) {
            _fill_read_cache_kv_ror(&ror_list[2], (RaceHashSlot *)(&ctx->cache_slot), (uint64_t)ctx->local_cache_addr);
            k += 1;
        }
    }

    network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);

    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    ctx->is_finished = false;
    return;
}

void Client::_kv_update_read_kv_from_buckets(KVReqCtx * ctx) {
    int ret = 0;
    if(ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }
    if ((define::cacheLevel == 1) && ctx->cache_entry && ctx->cache_read_kv) {
        KVLogHeader * cached_header = (KVLogHeader *)ctx->local_cache_addr;
        if (_cache_entry_is_valid(ctx)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            if (CheckKey((void *)read_key_addr, cached_header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                cache_hit_num++;
                ctx->cache_entry->acc_cnt++; // update cache counter
                ctx->is_prev_kv_hit = true;
                ctx->prev_ver = cached_header->version.val;
                return;
            }
        }
        cache_mis_num++;
        ctx->cache_entry->miss_cnt++;
    }

    _find_kv_in_buckets(ctx);
    if (ctx->kv_read_addr_list.size() == 0) {
        if (ctx->req_type != KV_OP_INSERT) {
            printf("ctx->kv_read_addr_list.size() == 0\n");
            ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
            ctx->is_finished = true;
        } 
        return;
    }

    int k = ctx->kv_read_addr_list.size();
    RdmaOpRegion * ror_list = new RdmaOpRegion[k];
    _fill_read_kv_ror(ror_list, ctx->kv_read_addr_list);
    network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);
    
    delete[] ror_list;
    ctx->is_finished = false;

    return;
}

void Client::_kv_update_write_kv_and_prepare_cas(KVReqCtx * ctx) {
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    // get previous ver
    int32_t match_idx;
    if (ctx->is_prev_kv_hit || ctx->is_curr_kv_hit) {
        ctx->has_previous = true;
    } else {
        match_idx = _find_match_slot(ctx);
        if (match_idx == -1) {
            ctx->has_previous = false;
            if (ctx->req_type != KV_OP_INSERT) {
                printf("match_idx == -1\n");
                ctx->is_finished = true;
                ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
                return;
            } else {
                _find_empty_slot(ctx);
                if (ctx->bucket_idx == -1) {
                    printf("bucket is full\n");
                    ctx->is_finished = true;
                    ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
                    return;
                }
            }
        } else {
            ctx->has_previous = true;
        }
    }

    if (ctx->req_type == KV_OP_INSERT) {
        assert(ctx->has_previous == false);  // [DEBUG] currently doesn't test INSERT after DELETE
    }

    // prepare cas
    uint64_t remote_slot_addr[define::replicaIndexNum];
    RaceHashSlot * old_slot = nullptr, * new_slot = nullptr;
    _prepare_old_slot(ctx, remote_slot_addr, &old_slot, &new_slot);

    ctx->req_type = ctx->req_type;
    ctx->prev_ver = old_slot->atomic.version;
    _alloc_new_kv(ctx);

    _fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, old_slot, new_slot);
    _fill_cas_addr(ctx, remote_slot_addr, old_slot, new_slot);
    
    _write_new_kv(ctx);
}

void Client::_kv_update_cas_primary(KVReqCtx * ctx) {
    if(ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    _modify_primary_idx(ctx);
    return;
}

bool Client::_cache_entry_is_valid(KVReqCtx * ctx) {
    assert(ctx->cache_entry != NULL);
    if (define::cacheLevel == 2) {
        RaceHashSlot & cache_slot = ctx->cache_slot;
        RaceHashSlot * local_slot = (RaceHashSlot *)ctx->local_slot_addr;
        return IsEqualSlot(local_slot, &cache_slot);
    } else {
        _get_local_bucket_info(ctx);

        RaceHashSlot * local_slot;
        AddrCacheEntry * cache_entry = ctx->cache_entry;
        assert(cache_entry != NULL);

        uint64_t r_slot_addr = cache_entry->r_slot_addr.load();
        // in first combined bucket
        if (r_slot_addr >= ctx->tbl_addr_info.f_bucket_addr[0] && 
            r_slot_addr < ctx->tbl_addr_info.f_bucket_addr[0] + (sizeof(RaceHashBucket) * 2)) {
            uint64_t slot_off = r_slot_addr - ctx->tbl_addr_info.f_bucket_addr[0];
            local_slot = (RaceHashSlot *)(((uint64_t)ctx->f_com_bucket) + slot_off);
        }
        else {
            assert(r_slot_addr >= ctx->tbl_addr_info.s_bucket_addr[0]);
            assert(r_slot_addr < (ctx->tbl_addr_info.s_bucket_addr[0] + (sizeof(RaceHashBucket) * 2)));
            uint64_t slot_off = r_slot_addr - ctx->tbl_addr_info.s_bucket_addr[0];
            local_slot = (RaceHashSlot *)(((uint64_t)ctx->s_com_bucket) + slot_off);
        }
        RaceHashSlot & cache_slot = ctx->cache_slot;
        return IsEqualSlot(local_slot, &cache_slot);
    }
}

bool Client::_need_degrade_search(uint64_t remote_addr, uint32_t server_id) {
    if (server_id != this->crash_server_id)
        return false;
    MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr_degrade(remote_addr, server_id, (uint64_t)degrade_buf, block_size);
    assert(block_meta->block_role == DATA_BLOCK);
    return block_meta->is_broken;
}

void Client::_find_kv_in_buckets_degrade(KVReqCtx * ctx) {
    _get_local_bucket_info(ctx);
    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    ctx->kv_recover_addr_list.clear();
    ctx->kv_idx_list.clear();

    // search all kv pair that finger print matches
    for (int i = 0; i < 4; i ++) {
        assert(ctx->bucket_arr[i]->local_depth == ctx->hash_info.local_depth);
        for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
            if (ctx->slot_arr[i][j].atomic.fp == ctx->hash_info.fp &&
                ctx->slot_arr[i][j].atomic.offset != 0) {
                KVRecoverAddr cur_kv_addr;
                cur_kv_addr.r_kv_addr = ConvertOffsetToAddr(ctx->slot_arr[i][j].atomic.offset);
                cur_kv_addr.server_id = ctx->slot_arr[i][j].atomic.server_id;
                cur_kv_addr.l_kv_addr = local_kv_buf_addr;
                local_kv_buf_addr += define::subblockSize;
                if (_need_degrade_search(cur_kv_addr.r_kv_addr, cur_kv_addr.server_id)) {
                    cur_kv_addr.need_xor = true;
                    _prepare_kv_recover_addr(cur_kv_addr, local_kv_buf_addr);
                } else {
                    cur_kv_addr.need_xor = false;
                    cur_kv_addr.local_addr_count = 1;
                }
                ctx->kv_recover_addr_list.push_back(cur_kv_addr);
                ctx->kv_idx_list.push_back(std::make_pair(i, j));
            }
        }
    }
}

void Client::_prepare_kv_recover_addr(KVRecoverAddr & ctx, uint64_t & local_kv_buf_addr) {
    assert(ctx.need_xor == true);
    uint64_t primar_addr = ctx.r_kv_addr;
    uint32_t primar_server_id = ctx.server_id;
    uint64_t primar_offset, local_offset;
    uint64_t parity_offsets[2], parity_addrs[2];
    uint32_t parity_server_ids[2];
    uint32_t block_count = 0;

    _convert_addr_to_offset(primar_addr, &primar_offset, &local_offset);
    _get_parity_slab_offsets(primar_offset, primar_server_id, parity_offsets, parity_server_ids);
    _convert_offset_to_addr(&parity_addrs[0], parity_offsets[0], local_offset);
    ctx.remote_server_id[block_count] = parity_server_ids[0];
    ctx.remote_addr[block_count] = parity_addrs[0];
    ctx.local_addr[block_count] = local_kv_buf_addr;
    block_count++;
    local_kv_buf_addr += define::subblockSize;

    std::vector<uint32_t> primary_server_ids;
    std::vector<uint64_t> primary_offsets;
    std::vector<uint64_t> primary_addrs;
    _get_primary_slab_offsets(parity_offsets[0], parity_server_ids[0], primary_server_ids, primary_offsets);
    
    MMBlockMeta * parity_meta = (MMBlockMeta *)get_meta_addr_degrade(parity_addrs[0], parity_server_ids[0], (uint64_t)degrade_buf, block_size);
    assert(parity_meta->block_role == PARITY_BLOCK);

    for (int i = 0; i < primary_server_ids.size(); i++) {
        uint64_t primary_addr;
        _convert_offset_to_addr(&primary_addr, primary_offsets[i], local_offset);
        primary_addrs.push_back(primary_addr);

        uint8_t xor_idx = _get_index_in_xor_map(primary_addr);
        if (parity_meta->parity.xor_map & (1 << xor_idx) || parity_meta->parity.delta_addr[xor_idx] > 0) {
            ctx.remote_server_id[block_count] = primary_server_ids[i];
            ctx.remote_addr[block_count] = primary_addrs[i];
            ctx.local_addr[block_count] = local_kv_buf_addr;
            block_count++;
            local_kv_buf_addr += define::subblockSize;
            ctx.prim_idxs.push_back(xor_idx);
        }
    }

    for (int i = 0; i < define::replicaNum; i++) {
        if (parity_meta->parity.delta_addr[i] > 0) {
            ctx.remote_server_id[block_count] = parity_server_ids[0];
            ctx.remote_addr[block_count] = parity_meta->parity.delta_addr[i] + (local_offset % block_size);
            ctx.local_addr[block_count] = local_kv_buf_addr;
            block_count++;
            local_kv_buf_addr += define::subblockSize;
            ctx.delt_idxs.push_back(i);
        }
    }
    ctx.local_addr_count = block_count;
    if (define::codeMethod == XOR_CODE && block_count == 1)
        ctx.local_addr[0] = ctx.l_kv_addr;
}

void Client::_get_primary_slab_offsets(uint64_t offset, uint32_t server_id, std::vector<uint32_t> & primary_server_ids, std::vector<uint64_t> & primary_offsets) {
    assert(((offset - HASH_AREA_BORDER) % slab_size == 0));

    auto it = parity_slab_map.find(offset | server_id);
    if (it == parity_slab_map.end()) {
        printf("parity slab offset wrong. offset: %lx, server_id: %x\n", offset, server_id);
        exit(1);
    }
    for (int i = 0; i < it->second.size(); i ++) {
        uint32_t cur_server_id = it->second[i] & 0xFF;
        if(cur_server_id == this->crash_server_id)
            continue;
        primary_server_ids.push_back(cur_server_id);
        primary_offsets.push_back((it->second[i] >> 8) << 8);
    }
    assert(primary_offsets.size() == define::memoryNodeNum - 3);
}

void Client::_fill_recover_kv_ror(RdmaOpRegion * ror, const std::vector<KVRecoverAddr> & r_addr_list) {
    int k = 0;
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        const KVRecoverAddr & rec = r_addr_list[i];
        if (rec.need_xor) {
            for (uint32_t j = 0; j < rec.local_addr_count; j++) {
                ror[k].remote_addr = rec.remote_addr[j];
                ror[k].remote_sid = rec.remote_server_id[j];
                ror[k].local_addr = rec.local_addr[j];
                ror[k].size = define::subblockSize;
                k++;
            }
        } else {
            ror[k].remote_addr = rec.r_kv_addr;
            ror[k].remote_sid = rec.server_id;
            ror[k].local_addr = rec.l_kv_addr;
            ror[k].size = define::subblockSize;
            k++;
        }
    }
}

void Client::_rs_update(uint64_t delta_addr, uint64_t parity_addr, size_t length, uint32_t data_idx) {
    assert(data_idx < define::codeK);
    uint8_t *parity_addr_ptr[1];
    parity_addr_ptr[0] = (uint8_t *)parity_addr;
    ec_encode_data_update(length, define::codeK, define::codeP, data_idx, encode_table, (uint8_t *)delta_addr, parity_addr_ptr);
}

void Client::_xor_one_kv(KVRecoverAddr & ctx) {
    if (define::codeMethod == XOR_CODE) {
        void *buffs[16];
        if (ctx.local_addr_count <= 1) {
            return;
        }
        for (int i = 0; i < ctx.local_addr_count; i++) {
            buffs[i] = (void *)ctx.local_addr[i];
        }
        buffs[ctx.local_addr_count] = (void *)ctx.l_kv_addr;
        xor_gen(ctx.local_addr_count + 1, define::subblockSize, buffs);
    } else {
        assert(define::codeMethod == RS_CODE);
        assert(ctx.local_addr_count == (1 + ctx.prim_idxs.size() + ctx.delt_idxs.size()));
        if (ctx.delt_idxs.size() > 0) {
            // update parity
            for (int i = 0; i < ctx.delt_idxs.size(); i++) {
                uint8_t buf_idx = i + 1 + ctx.prim_idxs.size();
                _rs_update(ctx.local_addr[buf_idx], ctx.local_addr[0], define::subblockSize, ctx.delt_idxs[i]); 
            }
        }
        uint8_t *recov[3], *coding[1];
        uint8_t recov_cnt = 0;
        uint8_t *blk_recov[3] = {NULL};
        assert(ctx.prim_idxs.size() < 3);
        for (int i = 0; i < ctx.prim_idxs.size(); i++) {
            uint8_t idx = ctx.prim_idxs[i];
            uint64_t l_addr = ctx.local_addr[1 + i];
            blk_recov[idx] = (uint8_t *)l_addr;
        }
        uint8_t crash_idx = _get_index_in_xor_map(ctx.r_kv_addr);   assert(crash_idx < 3);
        for (int i = 0; i < 3; i++) {
            if (crash_idx == i) {
                continue;
            }
            else {
                if (blk_recov[i] != NULL)
                    recov[recov_cnt] = blk_recov[i];
                else
                    recov[recov_cnt] = temp_buff_list[recov_cnt];
                recov_cnt++;
            }
        }
        assert(recov_cnt == 2);
        recov[2] = (uint8_t *)ctx.local_addr[0];   // last be the parity block
        coding[0] = (uint8_t *)ctx.l_kv_addr;
        ec_encode_data(define::subblockSize, define::codeK, 1, decode_table[crash_idx], recov, coding);
    }
}

void Client::_xor_all_kv(std::vector<KVRecoverAddr> & recover_list) {
    
    for (KVRecoverAddr & ctx : recover_list) {
        if (!ctx.need_xor)
            continue;
        assert(ctx.local_addr_count >= 1);
        _xor_one_kv(ctx);
    }
}

int32_t Client::_find_match_kv_idx_degrade(KVReqCtx * ctx) {
    int32_t ret = 0;

    for (size_t i = 0; i < ctx->kv_recover_addr_list.size(); i ++) {
        uint64_t read_key_addr = ctx->kv_recover_addr_list[i].l_kv_addr + sizeof(KVLogHeader);
        uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
        KVLogHeader * header = (KVLogHeader *)ctx->kv_recover_addr_list[i].l_kv_addr;
        if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
            ctx->prev_ver = header->version.val;
            return i;
        }
    }
    
    return -1;
}

void Client::_sim_gc_fetch_block(KVReqCtx * ctx, uint64_t remote_addr, uint32_t server_id) {
    RdmaOpRegion ror;
    ror.remote_addr = remote_addr;
    ror.remote_sid = server_id;
    ror.local_addr = (uint64_t)this->sim_gc_buf;
    ror.size = block_size;
    network_manager->rdma_read_batches_sync(&ror, 1, _get_coro_ctx(ctx));
}

void Client::_sim_gc_xor_kv(KVReqCtx * ctx, SubblockInfo & alloc_subblock) {
    uint64_t tmp_addr = (uint64_t)ctx->kv_info->l_addr;
    assert((tmp_addr % 64) == 0);
    uint64_t par_addr = (uint64_t)this->sim_gc_buf;
    xor_add_buffers(tmp_addr, par_addr, define::subblockSize);
}

void Client::_cache_read_prev_kv(KVReqCtx * ctx) {
    assert(define::cacheLevel == 2);
    int ret = 0;
    int k = 0;
    RdmaOpRegion ror_list[3];
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }
    
    _cache_search(ctx);
    if (ctx->cache_entry) {
        if (ctx->cache_read_kv) {
            _fill_read_cache_kv_ror(&ror_list[0], (RaceHashSlot *)(&ctx->cache_slot), (uint64_t)ctx->local_cache_addr);
            _fill_read_slot_ror(&ror_list[1], ctx->cache_entry->server_id, ctx->cache_entry->r_slot_addr, (uint64_t)ctx->local_slot_addr);
            k = 2;
        } else {
            _fill_read_slot_ror(&ror_list[0], ctx->cache_entry->server_id, ctx->cache_entry->r_slot_addr, (uint64_t)ctx->local_slot_addr);
            k = 1;
        }
        network_manager->rdma_read_batches_sync(ror_list, k, ctx->coro_ctx);
    }
    
    if (ctx->cache_entry && ctx->cache_read_kv) {
        KVLogHeader * cached_header = (KVLogHeader *)ctx->local_cache_addr;
        if (_cache_entry_is_valid(ctx)) {
            uint64_t read_key_addr = (uint64_t)ctx->local_cache_addr + sizeof(KVLogHeader);
            uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
            if (CheckKey((void *)read_key_addr, cached_header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
                cache_hit_num++;
                ctx->cache_entry->acc_cnt++; // update cache counter
                ctx->is_prev_kv_hit = true;
                return;
            }
        }
        cache_mis_num++;
        ctx->cache_entry->miss_cnt++;
    }
    return;
}

void Client::_cache_read_curr_kv(KVReqCtx * ctx) {
    int ret = 0;
    if (ctx->is_finished) {
        return;
    }
    if (*(ctx->should_stop)) {
        ctx->is_finished = true;
        return;
    }

    if (ctx->cache_entry) {
        RdmaOpRegion ror_list[1];
        _fill_read_kv_ror(ror_list, ctx, (RaceHashSlot *)ctx->local_slot_addr);
        network_manager->rdma_read_batches_sync(ror_list, 1, ctx->coro_ctx);

        uint64_t read_key_addr = (uint64_t)ctx->local_kv_addr + sizeof(KVLogHeader);
        uint64_t local_key_addr = (uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader);
        KVLogHeader * header = (KVLogHeader *)ctx->local_kv_addr;
        if (CheckKey((void *)read_key_addr, header->key_length, (void *)local_key_addr, ctx->kv_info->key_len)) {
            ctx->is_curr_kv_hit = true;
            addr_cache->cache_update(ctx->key_str, ctx->tbl_addr_info.server_id[0], (RaceHashSlot *)ctx->local_slot_addr, ctx->cache_entry->r_slot_addr);
        }
    }
    
    return;
}

void Client::init_kv_req_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops, CoroContext *coro_ctx) {
    void * coro_local_addr = (void *)coro_local_addr_list[coro_id];
    for (uint32_t i = 0; i < num_ops; i ++) {
        uint32_t kv_req_idx = kv_req_st_idx + i;
        KVReqCtx * kv_req_ctx = &kv_req_ctx_list[kv_req_idx];
        kv_req_ctx->coro_id = coro_id;
        kv_req_ctx->coro_ctx = coro_ctx;
        kv_req_ctx->use_cache = true;

        kv_req_ctx->local_bucket_addr = (RaceHashBucket *)coro_local_addr;
        kv_req_ctx->local_kv_addr = (void *)((uint64_t)kv_req_ctx->local_bucket_addr + 16 * sizeof(RaceHashBucket));
        kv_req_ctx->local_cas_target_value_addr = (void *)((uint64_t)kv_req_ctx->local_kv_addr + 128 * KB);
        kv_req_ctx->local_cas_return_value_addr = (void *)((uint64_t)kv_req_ctx->local_cas_target_value_addr + 16 * sizeof(RaceHashSlot));
        kv_req_ctx->local_slot_addr = (void *)((uint64_t)kv_req_ctx->local_cas_return_value_addr + 16 * sizeof(RaceHashSlot));
        kv_req_ctx->local_cache_addr = (void *)((uint64_t)kv_req_ctx->local_slot_addr + 128 * KB);
    }
}


// prepare requests for following load and trans (both micro and macro workloads)
int Client::load_kv_requests(uint32_t st_idx, int32_t num_ops, std::string workload_name, std::string op_type) {
    if (req_total_num != 0) {
        free(kv_info_list);
        free(kv_req_ctx_list);
        req_total_num = 0;
        req_local_num = 0;
    }
    
    FILE * workload_file = NULL;   // not used in micro
    char operation_buf[32];
    char table_buf[32];
    char key_buf[128];
    char value_buf[128];

    bool is_micro = is_micro_test(workload_name);
    if (!is_micro) {
        assert(workload_name != "");
        workload_file = fopen(workload_name.c_str(), "r");
        assert(workload_file != NULL);

        while (fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf) != EOF)
            req_total_num++;
        rewind(workload_file);

        if (num_ops == -1)
            req_local_num = req_total_num;
        else
            req_local_num = (st_idx + num_ops > req_total_num) ? req_total_num - st_idx : num_ops;
    } else {
        assert(op_type != "");
        st_idx = 0;
        req_total_num = req_local_num = num_ops;
    }

    kv_info_list    = (KVInfo *)malloc(sizeof(KVInfo) * req_local_num);
    kv_req_ctx_list = (KVReqCtx *)malloc(sizeof(KVReqCtx) * req_local_num);
    assert((kv_info_list != NULL) && (kv_req_ctx_list != NULL));

    memset(kv_info_list, 0, sizeof(KVInfo) * req_local_num);
    memset(kv_req_ctx_list, 0, sizeof(KVReqCtx) * req_local_num);
    uint64_t input_buf_ptr = (uint64_t)input_buf;

    uint32_t used_len = 0;
    int idx = 0;

    for (int i = 0; i < st_idx + req_local_num; i ++) {
        if (!is_micro) {
            int r = fscanf(workload_file, "%s %s %s", operation_buf, table_buf, key_buf);
            assert(r == 3);
            sprintf(value_buf, "initial-value-%d", i);
            if (i < st_idx)
                continue;
            op_type = operation_buf;
        } else {
            sprintf(key_buf, "micro-%03d-%02d", my_unique_id, i);
            sprintf(value_buf, "initial-value-%d", i);
        }
        
        // prepare local KV addr
        uint32_t all_len = round_up(sizeof(KVLogHeader) + strlen(key_buf) + strlen(value_buf) + sizeof(KVLogTail), 64);
        KVLogHeader * kv_log_header = (KVLogHeader *)input_buf_ptr;
        void * key_st_addr = (void *)(input_buf_ptr + sizeof(KVLogHeader));
        void * value_st_addr = (void *)((uint64_t)key_st_addr + strlen(key_buf));
        KVLogTail * kv_log_tail = (KVLogTail *)((uint64_t)value_st_addr + strlen(value_buf));
        
        // write local KV data
        kv_log_header->key_length = strlen(key_buf);
        kv_log_header->value_length = strlen(value_buf);
        memcpy(key_st_addr, key_buf, strlen(key_buf));
        memcpy(value_st_addr, value_buf, strlen(value_buf));
        kv_log_header->op = KV_OP_INSERT;

        // fill kv info
        kv_info_list[idx].key_len = strlen(key_buf);
        kv_info_list[idx].value_len = strlen(value_buf);
        kv_info_list[idx].l_addr  = (void *)input_buf_ptr;
        kv_info_list[idx].lkey = local_buf_mr->lkey;

        // init request context
        KVReqCtx * req_ctx = &kv_req_ctx_list[idx];
        KVInfo * kv_info = &kv_info_list[idx];
        req_ctx->kv_info = kv_info;
        req_ctx->lkey = local_buf_mr->lkey;
        req_ctx->key_str = key_buf;
        if (op_type == "INSERT") {
            req_ctx->req_type = KV_OP_INSERT;
        } else if (op_type == "DELETE") {
            req_ctx->req_type = KV_OP_DELETE;
        } else if (op_type == "UPDATE") {
            req_ctx->req_type = KV_OP_UPDATE;
        } else if (op_type == "READ") {
            req_ctx->req_type = KV_OP_SEARCH;
        } else {
            req_ctx->req_type = KV_OP_SEARCH;
        }

        input_buf_ptr += all_len;
        used_len += all_len;
        if (used_len >= define::inputBufferSize) {
            printf("Overflow.\n");
        }
        idx++;
    }
    return 0;
}

void Client::client_barrier(const std::string &str) {
    network_manager->client_barrier(str);
}

void Client::start_degrade(uint32_t crash_server_id) {
    this->crash_server_id = crash_server_id;

    RdmaOpRegion ror;
    ror.local_addr = (uint64_t)degrade_buf;
    ror.remote_sid = crash_server_id;
    ror.remote_addr = define::baseAddr;
    ror.size = get_total_meta_area_size(block_size);
    ror.op_code = IBV_WR_RDMA_READ;
    network_manager->rdma_read_batches_sync(&ror, 1);
}

void Client::run_coroutine(volatile bool *should_stop, WorkFunc work_func, uint32_t coro_num) {
    using namespace std::placeholders;
    lock_coro_id = 0xFF;
    // statics initialize
    req_finish_num = 0, req_failed_num = 0, req_modify_num = 0, req_cas_num = 0;
    std::fill(req_search_num, req_search_num + 5, 0);
    cache_hit_num = 0, cache_mis_num = 0, cur_valid_kv_sz = 0, raw_valid_kv_sz = 0;
    for (int i = 0; i < 4; i++) {
        memset(req_latency[i], 0, LATENCY_WINDOWS * sizeof(uint64_t));
    }

    my_coro_num = coro_num;
    for (int i = 0; i < my_coro_num; ++i) {
        worker[i] = CoroCall(std::bind(&Client::coro_worker, this, _1, i, should_stop, work_func));
    }
    worker[coro_num] = CoroCall(std::bind(&Client::coro_gc, this, _1, coro_num, should_stop));

    master = CoroCall(std::bind(&Client::coro_master, this, _1, should_stop));

    master();
}

void Client::coro_worker(CoroYield &yield, int coro_id, volatile bool *should_stop, WorkFunc work_func) {
    CoroContext ctx;
    ctx.coro_id = coro_id;
    ctx.master = &master;
    ctx.yield = &yield;

    uint32_t req_coro_num = req_local_num / my_coro_num;
    uint32_t req_st_idx = req_coro_num * coro_id;
    init_kv_req_space(coro_id, req_st_idx, req_coro_num, &ctx);

    int ret = 0;
    void * search_addr = NULL;
    uint32_t cnt = 0;
    std::unordered_map<std::string, bool> inserted_key_map;
    timespec req_st, req_ed;
    
    while (!(*should_stop)) {
        uint32_t idx = cnt % req_coro_num;
        KVReqCtx * ctx = &kv_req_ctx_list[idx + req_st_idx];
        ctx->coro_id = coro_id;
        ctx->should_stop = should_stop;

        clock_gettime(CLOCK_REALTIME, &req_st);

        switch (ctx->req_type) {
        case KV_OP_SEARCH:
            search_addr = work_func(this, ctx).value_addr;
            if (search_addr == NULL) {
                this->req_failed_num++;
            }
            break;
        case KV_OP_INSERT:
            if (inserted_key_map[ctx->key_str] == true) {
                char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                modify[4]++;
                ctx->key_str[4]++;
            }
            ret = work_func(this, ctx).ret_code;
            if (ret == KV_OPS_FAIL_RETURN) {
                this->req_failed_num++;
            }
            this->req_modify_num++;
            inserted_key_map[ctx->key_str] = true;
            break;
        case KV_OP_UPDATE:
            ret = work_func(this, ctx).ret_code;
            if (ret == KV_OPS_FAIL_RETURN) {
                this->req_failed_num++;
            }
            this->req_modify_num++;
            break;
        case KV_OP_DELETE:
            ret = work_func(this, ctx).ret_code;
            if (ret == KV_OPS_FAIL_RETURN) {
                this->req_failed_num++;
            }
            this->req_modify_num++;
            break;
        default:
            work_func(this, ctx);
            break;
        }

        clock_gettime(CLOCK_REALTIME, &req_ed);
        uint64_t latency_ns = (req_ed.tv_sec - req_st.tv_sec) * 1000000000ull + (req_ed.tv_nsec - req_st.tv_nsec);
        uint64_t latency_us = latency_ns / 1000;
        if (latency_us >= LATENCY_WINDOWS) {
            latency_us = LATENCY_WINDOWS - 1;
        }
        req_latency[ctx->req_type-1][latency_us]++;
        
        if (ret == KV_OPS_FAIL_REDO) {
            cnt--;
            this->req_finish_num--;
        }
        cnt++;
        this->req_finish_num++;
    }
}

void Client::coro_gc(CoroYield &yield, int coro_id, volatile bool *should_stop) {
    while (!(*should_stop)) {
        // printf("start free\n");
        std::unordered_map<std::string, uint64_t> gc_map(this->free_bit_map);
        this->free_bit_map.clear();
        for (auto it = gc_map.begin(); it != gc_map.end(); it++) {
            // TODO: use 2-sided RPC + 1-sided WRITE to batch free stale KVs, first WRITE the bitmaps to a pre-allocated region, then notify the server to process
        }

        auto last_time = std::chrono::high_resolution_clock::now();
        busy_waiting_queue.push(std::make_pair(coro_id, [&last_time](){
            auto curr_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(curr_time - last_time);
            if (duration.count() >= 10) {    // GC every 10 seconds
                last_time = curr_time;
                return true;
            }
            return false;
        }));
        yield(master);
    }
}

void Client::coro_master(CoroYield &yield, volatile bool *should_stop) {
    for (int i = 0; i < my_coro_num + 1; ++i) {
        yield(worker[i]);
    }
    std::deque<uint64_t> wait_coros;
    while (!(*should_stop)) {
        uint64_t next_coro_id = 0xFF;
        ibv_wc wc;
        // try to get a new coro_id
        if (network_manager->poll_cq_once(&wc, network_manager->get_ib_cq())) {
            assert(wc.opcode != IBV_WC_RECV);
            next_coro_id = wc.wr_id;
            assert(next_coro_id < my_coro_num);
        } else if (network_manager->poll_cq_once(&wc, network_manager->get_rpc_recv_cq())) {
            assert(wc.opcode == IBV_WC_RECV);
            KVMsg *reply = (KVMsg *)network_manager->rpc_get_message();
            uint16_t reply_coro_id = reply->co_id;
            assert(reply_coro_id < my_coro_num);
            if (network_manager->rpc_fill_wait_reply(reply_coro_id, reply)) {
                next_coro_id = reply_coro_id;
            }
        }

        // if get a new coro_id, process it
        if (next_coro_id < my_coro_num) {
            if (subblock_free_queue.size() > 0) {
                yield(worker[next_coro_id]);
            } else {    // subblock_free_queue.size() == 0
                assert(lock_coro_id != 0xFF);
                if (lock_coro_id == next_coro_id) {
                    yield(worker[next_coro_id]);
                } else {
                    wait_coros.push_back(next_coro_id);
                }
            }
        } else {    // next_coro_id == 0xFF
            if (subblock_free_queue.size() > 0 && wait_coros.size() > 0) {
                next_coro_id = wait_coros.front();
                wait_coros.pop_front();
                yield(worker[next_coro_id]);
            }
        }

        if (!busy_waiting_queue.empty()) {
            auto next = busy_waiting_queue.front();
            busy_waiting_queue.pop();
            next_coro_id = next.first;
            if (next.second()) {
                yield(worker[next_coro_id]);
            }
            else {
                busy_waiting_queue.push(next);
            }
        }
    }
}

