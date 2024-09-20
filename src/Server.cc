#include "Server.h"
#include "KvUtils.h"

std::mutex xor_mutex;
std::condition_variable xor_cv;
std::mutex ckpt_send_mutex, ckpt_recv_mutex;
std::condition_variable ckpt_send_cv, ckpt_recv_cv;

void * server_main(void * server_main_args) {
    ServerMainArgs * args = (ServerMainArgs *)server_main_args;
    Server * server_instance = args->server;
    
    // start working
    return server_instance->thread_main();   
}

Server::Server(uint8_t server_id, bool is_recovery, uint32_t ckpt_interval, uint32_t block_size) {
    this->my_server_id = server_id;
    this->need_stop = 0;
    this->is_recovery = is_recovery;
    this->ckpt_interval = ckpt_interval;
    this->ckpt_start = false;
    this->hash_ckpt_idx = 0;
    this->hash_ckpt_send = 0;
    this->hash_ckpt_recv = 0;
    this->check_valid_kv_cnt = 0;
    this->check_lost_r_kv_cnt = 0;
    this->check_lost_l_kv_cnt = 0;

    this->block_size = block_size;
    this->subblock_num = block_size / define::subblockSize;
    this->bmap_subblock_num = (((subblock_num + 7) / 8 + 1023) / define::subblockSize);
    this->data_subblock_num = subblock_num - bmap_subblock_num;
    this->alloc_watermark = data_subblock_num / 2;

    if (define::codeMethod == RS_CODE) {
        _init_rs_code();
    }

    createContext(&rdma_ctx);

    _init_memory();
    _init_layout();
    
    if (!is_recovery) {
        network_manager = new NetworkManager(SERVER, &rdma_ctx, my_server_id, 0, 0, ib_mr, false);
        _init_hashtable();
        _init_block();
        _init_ckpt();
    } else {
        network_manager = new NetworkManager(SERVER, &rdma_ctx, my_server_id, 0, 0, ib_mr, true);
        _recover_server();
    }
}

Server::~Server() {
    munmap(data_raw, define::serverMemorySize);
    delete network_manager;
}

void Server::_init_slab_map() {
    assert(define::memoryNodeNum > 2);
    slab_num = define::memoryNodeNum;

    uint64_t memblock_area_total_len = round_down(define::serverMemorySize - HASH_AREA_BORDER, slab_num * block_size);
    uint64_t memblock_area_valid_len = memblock_area_total_len / slab_num * (slab_num - 2);
    uint64_t memblock_area_parit_len = memblock_area_total_len / slab_num * 2;
    
    slab_size = memblock_area_total_len / slab_num;
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

int Server::_recover_server() {
    _init_slab_map();
    recover_buf = (void *)((uint64_t)data_raw + client_kv_pari_off);
    struct timeval recover_st;
    struct timeval recover_meta_et;
    struct timeval recover_hash_et;
    struct timeval recover_data_et;

    gettimeofday(&recover_st, NULL);

    _recover_metadata();
    gettimeofday(&recover_meta_et, NULL);
    
    _recover_index();
    gettimeofday(&recover_hash_et, NULL);
    
    _recover_block();
    gettimeofday(&recover_data_et, NULL);

    uint64_t meta_recover_time_us  = time_spent_us(&recover_st,        &recover_meta_et);
    uint64_t hash_recover_time_us  = time_spent_us(&recover_meta_et,   &recover_hash_et);
    uint64_t data_recover_time_us  = time_spent_us(&recover_hash_et,   &recover_data_et);
    
    if (meta_recover_time_us > 1000000) {
        printf("[WARNING] meta recover time error, please run again\n");
    }
    printf("meta recover: %lu\n",  meta_recover_time_us);   // us
    printf("hash recover: %lu\n",  hash_recover_time_us);   // us
    printf("data recover: %lu %lu\n",  local_block_list.size(), data_recover_time_us);   // us
    printf("check count: %lu\n",   total_check_count);
}

int Server::_recover_metadata() {
    uint32_t rem_server_id = (my_server_id + 1) % define::memoryNodeNum;

    RdmaOpRegion ror;
    ror.local_addr = define::baseAddr;
    ror.remote_sid = rem_server_id;
    ror.remote_addr = define::baseAddr;
    ror.size = get_total_meta_area_size(block_size);
    ror.op_code = IBV_WR_RDMA_READ;
    network_manager->rdma_read_batches_sync(&ror, 1, nullptr, network_manager->get_ckpt_cq());

    RdmaOpRegion ror_list[define::memoryNodeNum - 2];
    for (int i = 2; i < define::memoryNodeNum; i++) {
        uint32_t rem_server_id = (my_server_id + i) % define::memoryNodeNum;
        ror_list[i - 2].local_addr = get_parity_meta_area_addr(rem_server_id, client_kv_pari_off, block_size);
        ror_list[i - 2].remote_sid = rem_server_id;
        ror_list[i - 2].remote_addr = get_parity_meta_area_addr(rem_server_id, client_kv_pari_off, block_size);
        ror_list[i - 2].size = get_parity_meta_area_size_per_node(client_kv_pari_off, block_size);
        ror_list[i - 2].op_code = IBV_WR_RDMA_READ;
    }
    network_manager->rdma_read_batches_sync(ror_list, define::memoryNodeNum - 2, nullptr, network_manager->get_ckpt_cq());

    _recover_meta_state();
}

int Server::_recover_meta_state() {
    // recovery of normal block metadata
    int total_alloc = 0;
    MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr_with_bid(0, my_server_id, block_size);
    uint64_t block_addr = define::baseAddr + client_kv_area_off;
    for(int i = 0; i < this->num_blocks; i++) {
        if (block_meta->block_role == DATA_BLOCK) {
            block_meta->is_broken = 1;
            allocated_blocks[block_addr] = true;
            total_alloc ++;
        } else {
            block_meta->block_role = FREE_BLOCK;
            block_meta->normal.index_ver = 0;
            block_meta->normal.xor_idx = 0xFF;
            allocable_blocks.push(block_addr);
        }
        block_meta ++;
        block_addr += block_size;
    }
    
    // recovery of parity block metadata
    uint64_t parity_addr = define::baseAddr + client_kv_pari_off;
    MMBlockMeta * parity_meta = (MMBlockMeta *)get_meta_addr(parity_addr, my_server_id, block_size);
    for (int i = 0; i < this->num_parity_blocks; i++) {
        parity_meta->block_role = PARITY_BLOCK;
        std::fill(parity_meta->parity.delta_addr, parity_meta->parity.delta_addr + 3, 0);
        parity_meta->parity.xor_map = 0;
        parity_meta ++;
        parity_addr += block_size;
    }
    printf("recover_metadata done, totally have %d memory blocks in this MN\n", total_alloc);
}

int Server::_recover_index() {
    struct timeval time_1;
    struct timeval time_2;
    struct timeval time_3;
    struct timeval time_4;
    struct timeval time_5;

    gettimeofday(&time_1, NULL);
    _recover_old_index();
    _recover_subtable_state();

    gettimeofday(&time_2, NULL);
    _recover_local_new_block();
    gettimeofday(&time_3, NULL);
    _recover_remote_new_block();
    gettimeofday(&time_4, NULL);

    _recover_check_all();
    gettimeofday(&time_5, NULL);

    uint64_t old_index_us       = time_spent_us(&time_1,    &time_2);
    uint64_t local_block_us     = time_spent_us(&time_2,    &time_3);
    uint64_t remot_block_us     = time_spent_us(&time_3,    &time_4);
    uint64_t kv_check_us        = time_spent_us(&time_4,    &time_5);

    printf("index-old recover: %lu\n",  old_index_us);   // us
    printf("index-lblock recover: %lu %lu\n", local_block_list.size(), local_block_us);   // us
    printf("index-rblock recover: %lu %lu\n", remote_block_list.size(), remot_block_us);   // us
    printf("index-check recover: %lu\n",  kv_check_us);
}

int Server::_recover_old_index() {
    uint32_t rem_server_id = (my_server_id + 1) % define::memoryNodeNum;   // go to the next memory node

    RdmaOpRegion ror;
    ror.local_addr = define::baseAddr + client_hash_area_off;
    ror.remote_sid = rem_server_id;
    ror.remote_addr = hash_ckpt_addr[1];
    ror.size = HASH_AREA_LEN;
    ror.op_code = IBV_WR_RDMA_READ;
    network_manager->rdma_read_batches_sync(&ror, 1, nullptr, network_manager->get_ckpt_cq());

    // TODO: delete this check
    race_root = (RaceHashRoot *)(define::baseAddr + client_hash_area_off);
    for (int i = 0; i < 32; i++) {
        RaceHashSubtableEntry * entry_ptr = &(race_root->subtable_entry[i][0]);
        assert(entry_ptr->lock == 0);
        assert(entry_ptr->local_depth == 5);
        assert(entry_ptr->server_id < define::memoryNodeNum);
        uint64_t subtable_addr = HashIndexConvert40To64Bits(entry_ptr->pointer);
        uint64_t subtable_st_addr = _get_subtable_st_addr();
        assert(((subtable_addr - subtable_st_addr) % roundup_256(SUBTABLE_LEN)) == 0);
    }

    this->index_ver_before_crash = race_root->index_ver;
    printf("recover_index_ckpt done, old index_ver %lu\n", race_root->index_ver);
}

int Server::_recover_subtable_state() {
    // TODO: delete this check
    uint64_t subtable_addr = _get_subtable_st_addr();
    uint64_t max_subtables = (define::baseAddr + client_hash_area_off + client_hash_area_len - (uint64_t)subtable_addr) / roundup_256(SUBTABLE_LEN);
    uint32_t restore_count = 0;
    subtable_alloc_map.resize(max_subtables);
    uint32_t err_code = 0;
    for (int i = 0; i < max_subtables; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)subtable_addr + i * roundup_256(SUBTABLE_LEN);
        subtable_alloc_map[i] = 1;

        RaceHashBucket * bucket = (RaceHashBucket *)cur_subtable_addr;
        if (bucket->local_depth == 0) {
            subtable_alloc_map[i] = 0;
        } else {
            for (int j = 0; j < RACE_HASH_SUBTABLE_BUCKET_NUM; j++) {
                if (bucket[j].local_depth != RACE_HASH_INIT_LOCAL_DEPTH) {
                    err_code = 1;
                    // printf("recovery failed: subtable[%d], bucket[%d].local_depth != 5\n", i, j);
                }
                for (int k = 0; k < RACE_HASH_ASSOC_NUM; k++) {
                    if (bucket[j].slots[k].atomic.offset != 0) {
                        uint64_t ptr_addr = ConvertOffsetToAddr(bucket[j].slots[k].atomic.offset);
                        if (ptr_addr <= (HASH_AREA_BORDER + define::baseAddr)) {
                            err_code = 3;
                            // printf("recovery failed: subtable[%d], bucket[%d], slots[%d].pointer = %lld, too small\n", i, j, k, ptr_addr);
                        } else if (ptr_addr >= (HASH_AREA_BORDER + define::baseAddr + client_kv_area_len)) {
                            err_code = 4;
                            // printf("recovery failed: subtable[%d], bucket[%d], slots[%d].pointer = %lld, too big\n", i, j, k, ptr_addr);  
                        }
                    }
                }
            }
            restore_count++;
        }
    }
    if (err_code)
        printf("recover_subtable_state failed, err_code %d\n", err_code);
    else
        printf("recover_subtable_state done, totally restore %u subtables\n", restore_count);
    return 0;
}

int Server::_recover_block() {
    local_block_list.clear();
    recover_buf_ptr = (uint64_t)recover_buf;
    uint32_t block_cnt = get_normal_block_count_per_node(client_kv_pari_off, block_size);
    for(int i = 0; i < block_cnt; i++) {
        MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr_with_bid(i, my_server_id, block_size);
        if (block_meta->block_role == DATA_BLOCK && block_meta->normal.index_ver < this->index_ver_before_crash) {
            assert(block_meta->is_broken == 1);
            // block_meta->is_broken = 0;
            MMBlockRecoverContext new_recover;
            new_recover.primary_addr = get_block_addr_with_bid(i, block_size);
            _prepare_new_recover_context(&new_recover);
            local_block_list.push_back(new_recover);
        }
    }
    _xor_all_block(local_block_list);
}

void Server::_convert_addr_to_offset(uint64_t addr, uint64_t *offset, uint64_t *local_offset) {
    uint64_t total_offset = addr - define::baseAddr;
    assert(total_offset >= HASH_AREA_BORDER);
    *offset = HASH_AREA_BORDER + round_down(total_offset - HASH_AREA_BORDER, slab_size);
    *local_offset = (total_offset - HASH_AREA_BORDER) % slab_size;
}

void Server::_convert_offset_to_addr(uint64_t *addr, uint64_t offset, uint64_t local_offset) {
    *addr = define::baseAddr + offset + local_offset;
}

void Server::_get_parity_slab_offsets(uint64_t offset, uint32_t server_id, uint64_t parity_offsets[], uint32_t parity_server_ids[]) {
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

void Server::_get_primary_slab_offsets(uint64_t offset, uint32_t server_id, std::vector<uint32_t> & primary_server_ids, std::vector<uint64_t> & primary_offsets) {
    assert(((offset - HASH_AREA_BORDER) % slab_size == 0));

    auto it = parity_slab_map.find(offset | server_id);
    if (it == parity_slab_map.end()) {
        printf("parity slab offset wrong. offset: %lx, server_id: %x\n", offset, server_id);
        exit(1);
    }
    for (int i = 0; i < it->second.size(); i ++) {
        uint32_t cur_server_id = it->second[i] & 0xFF;
        if(cur_server_id == my_server_id)
            continue;
        primary_server_ids.push_back(cur_server_id);
        primary_offsets.push_back((it->second[i] >> 8) << 8);
    }
    assert(primary_offsets.size() == define::memoryNodeNum - 3);
}

uint64_t Server::_get_new_recover_block_addr() {
    uint64_t new_addr = recover_buf_ptr;
    recover_buf_ptr += block_size;
    if (recover_buf_ptr >= define::baseAddr + define::serverMemorySize) {
        printf("[WARNING] recover_buf_ptr reinit\n");
        recover_buf_ptr = (uint64_t)recover_buf;
    }
    return new_addr;
}

void Server::_prepare_new_recover_context(MMBlockRecoverContext * ctx) {
    uint64_t primar_addr = ctx->primary_addr;
    uint64_t primar_offset, local_offset;
    uint64_t parity_offsets[2], parity_addrs[2];
    uint32_t parity_server_ids[2];
    uint32_t block_count = 0;

    _convert_addr_to_offset(primar_addr, &primar_offset, &local_offset);
    _get_parity_slab_offsets(primar_offset, my_server_id, parity_offsets, parity_server_ids);
    _convert_offset_to_addr(&parity_addrs[0], parity_offsets[0], local_offset);
    ctx->remote_server_id[block_count] = parity_server_ids[0];
    ctx->remote_addr[block_count] = parity_addrs[0];
    ctx->local_addr[block_count] = _get_new_recover_block_addr();
    block_count++;

    std::vector<uint32_t> primary_server_ids;
    std::vector<uint64_t> primary_offsets;
    std::vector<uint64_t> primary_addrs;
    _get_primary_slab_offsets(parity_offsets[0], parity_server_ids[0], primary_server_ids, primary_offsets);
    
    MMBlockMeta * parity_meta = (MMBlockMeta *)get_meta_addr(parity_addrs[0], parity_server_ids[0], block_size);
    assert(parity_meta->block_role == PARITY_BLOCK);
    uint8_t xor_idx = _get_index_in_xor_map(primar_addr);
    
    for (int i = 0; i < primary_server_ids.size(); i++) {
        uint64_t primary_addr;
        _convert_offset_to_addr(&primary_addr, primary_offsets[i], local_offset);
        primary_addrs.push_back(primary_addr);

        uint8_t xor_idx = _get_index_in_xor_map(primary_addr);
        if (parity_meta->parity.xor_map & (1 << xor_idx) || parity_meta->parity.delta_addr[xor_idx] > 0) {
            ctx->remote_server_id[block_count] = primary_server_ids[i];
            ctx->remote_addr[block_count] = primary_addrs[i];
            ctx->local_addr[block_count] = _get_new_recover_block_addr();
            block_count++;
            ctx->prim_idxs.push_back(xor_idx);
        }
    }

    for (int i = 0; i < define::replicaNum; i++) {
        if (parity_meta->parity.delta_addr[i] > 0) {
            ctx->remote_server_id[block_count] = parity_server_ids[0];
            ctx->remote_addr[block_count] = parity_meta->parity.delta_addr[i];
            ctx->local_addr[block_count] = _get_new_recover_block_addr();
            block_count++;
            ctx->delt_idxs.push_back(i);
        }
    }
    ctx->local_addr_count = block_count;
    if (define::codeMethod == XOR_CODE && block_count == 1)
        ctx->local_addr[0] = ctx->primary_addr;
}

void Server::_xor_one_block(MMBlockRecoverContext * ctx) {
    if (define::codeMethod == XOR_CODE) {
        if (ctx->local_addr_count <= 1) {
            return;
        }
        void **buffs = new void*[ctx->local_addr_count + 1];
        for (int i = 0; i < ctx->local_addr_count; i++) {
            buffs[i] = (void *)ctx->local_addr[i];
        }
        buffs[ctx->local_addr_count] = (void *)ctx->primary_addr;
        xor_gen(ctx->local_addr_count + 1, block_size, buffs);
        delete[] buffs;
    } else {
        assert(define::codeMethod == RS_CODE);
        assert(ctx->local_addr_count == (1 + ctx->prim_idxs.size() + ctx->delt_idxs.size()));
        if (ctx->delt_idxs.size() > 0) {
            // update parity
            for (int i = 0; i < ctx->delt_idxs.size(); i++) {
                uint8_t buf_idx = i + 1 + ctx->prim_idxs.size();
                _rs_update(ctx->local_addr[buf_idx], ctx->local_addr[0], block_size, ctx->delt_idxs[i]); 
            }
        }
        uint8_t *recov[3], *coding[1];
        uint8_t recov_cnt = 0;
        uint8_t *blk_recov[3] = {NULL};
        assert(ctx->prim_idxs.size() < 3);
        for (int i = 0; i < ctx->prim_idxs.size(); i++) {
            uint8_t idx = ctx->prim_idxs[i];
            uint64_t l_addr = ctx->local_addr[1 + i];
            blk_recov[idx] = (uint8_t *)l_addr;
        }
        uint8_t crash_idx = _get_index_in_xor_map(ctx->primary_addr);   assert(crash_idx < 3);
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
        recov[2] = (uint8_t *)ctx->local_addr[0];   // last be the PARITY block
        coding[0] = (uint8_t *)ctx->primary_addr;
        ec_encode_data(block_size, define::codeK, 1, decode_table[crash_idx], recov, coding);
    }
}

void Server::_xor_all_block(std::vector<MMBlockRecoverContext> & recover_list) {
    int prev_batch_idx = -1;
    int batch_size = 0;
    for (int i = 0; i < recover_list.size(); i++) {
        if (((i + 1) % define::xorBlockBatch == 0) || (i == recover_list.size() - 1)) {
            int ror_cnt = 0;
            batch_size = i - prev_batch_idx;
            for (int j = 0; j < batch_size; j++) {
                ror_cnt += recover_list[i - j].local_addr_count;
            }
            RdmaOpRegion * ror = new RdmaOpRegion[ror_cnt];    assert(ror != nullptr);
            int base_idx = 0;
            for (int j = 0; j < batch_size; j++) {
                MMBlockRecoverContext & ctx = recover_list[i - j];
                for (int k = 0; k < ctx.local_addr_count; k++) {
                    ror[base_idx + k].local_addr = ctx.local_addr[k];
                    ror[base_idx + k].remote_addr = ctx.remote_addr[k];
                    ror[base_idx + k].remote_sid = ctx.remote_server_id[k];
                    ror[base_idx + k].size = block_size;
                }
                base_idx += ctx.local_addr_count;
            }
            int poll_num = network_manager->rdma_read_batches_async(ror, ror_cnt);

            if (prev_batch_idx > 0) {
                for (int j = 0; j < define::xorBlockBatch; j++) {
                    _xor_one_block(&recover_list[prev_batch_idx - j]);
                }
            }

            network_manager->poll_cq_sync(poll_num, network_manager->get_ckpt_cq());
            delete[] ror;

            prev_batch_idx = i;
        }
    }

    if (prev_batch_idx > 0) {
        for (int j = 0; j < batch_size; j++) {
            _xor_one_block(&recover_list[prev_batch_idx - j]);
        }
    }
}

int Server::_recover_local_new_block() {
    local_block_list.clear();
    recover_buf_ptr = (uint64_t)recover_buf;
    uint32_t block_cnt = get_normal_block_count_per_node(client_kv_pari_off, block_size);
    for(int i = 0; i < block_cnt; i++) {
        MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr_with_bid(i, my_server_id, block_size);
        if (block_meta->block_role == DATA_BLOCK && block_meta->normal.index_ver >= this->index_ver_before_crash) {
            assert(block_meta->is_broken == 1);
            block_meta->is_broken = 0;
            MMBlockRecoverContext new_recover;
            new_recover.is_remote = false;
            new_recover.primary_addr = get_block_addr_with_bid(i, block_size);
            _prepare_new_recover_context(&new_recover);
            local_block_list.push_back(new_recover);
            
            local_block_map[new_recover.primary_addr | my_server_id] = new_recover.primary_addr;
        }
    }
    _xor_all_block(local_block_list);
}

void Server::_read_all_block(std::vector<MMBlockRecoverContext> & recover_list) {
    int total_k = recover_list.size();
    RdmaOpRegion * ror = new RdmaOpRegion[total_k]; assert(ror != nullptr);
    for (int i = 0; i < total_k; i++) {
        ror[i].local_addr = recover_list[i].local_addr[0];
        ror[i].remote_addr = recover_list[i].remote_addr[0];
        ror[i].remote_sid = recover_list[i].remote_server_id[0];
        ror[i].size = block_size;
    }
    int st_idx = 0;
    while (st_idx < total_k) {
        int k = std::min(total_k - st_idx, (int)define::readBlockBatch);
        network_manager->rdma_read_batches_sync(&ror[st_idx], k, nullptr, network_manager->get_ckpt_cq());
        st_idx += k;
    }
    delete[] ror;
}

void Server::_rs_update(uint64_t delta_addr, uint64_t parity_addr, size_t length, uint32_t data_idx) {
    assert(data_idx < define::codeK);
    uint8_t *parity_addr_ptr[1];
    parity_addr_ptr[0] = (uint8_t *)parity_addr;
    ec_encode_data_update(length, define::codeK, define::codeP, data_idx, encode_table, (uint8_t *)delta_addr, parity_addr_ptr);
}

int Server::_recover_remote_new_block() {
    remote_block_list.clear();
    recover_buf_ptr = (uint64_t)recover_buf;
    for (int i = 1; i < define::memoryNodeNum; i++) {
        uint32_t tmp_sid = (my_server_id + i) % define::memoryNodeNum;
        uint32_t block_cnt = get_normal_block_count_per_node(client_kv_pari_off, block_size);
        for(int j = 0; j < block_cnt; j++) {
            MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr_with_bid(j, tmp_sid, block_size);
            if (block_meta->block_role == DATA_BLOCK && block_meta->normal.index_ver >= this->index_ver_before_crash) {
                MMBlockRecoverContext new_recover;
                new_recover.is_remote = true;
                new_recover.primary_addr = get_block_addr_with_bid(j, block_size);
                new_recover.local_addr[0] = _get_new_recover_block_addr();
                new_recover.remote_addr[0] = new_recover.primary_addr;
                new_recover.remote_server_id[0] = tmp_sid;
                remote_block_list.push_back(new_recover);

                local_block_map[new_recover.primary_addr | tmp_sid] = new_recover.local_addr[0];
            }
        }
    }
    _read_all_block(remote_block_list);
}

void Server::_fill_recover_kv_ror(RdmaOpRegion * ror, const std::vector<KVRecoverAddr> & r_addr_list) {
    int k = 0;
    for (size_t i = 0; i < r_addr_list.size(); i ++) {
        const KVRecoverAddr & rec = r_addr_list[i];
        if (rec.local_hit == false) {
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
}

void Server::_recover_prepare_kv_recover(KVRecoverAddr & ctx, uint64_t & local_kv_buf_addr) {
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
    
    MMBlockMeta * parity_meta = (MMBlockMeta *)get_meta_addr_degrade(parity_addrs[0], parity_server_ids[0], define::baseAddr, block_size);
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

void Server::_xor_one_kv(KVRecoverAddr & ctx) {
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

void Server::_xor_all_kv(std::vector<KVRecoverAddr> & recover_list) {
    
    for (KVRecoverAddr & ctx : recover_list) {
        if (!ctx.need_xor || ctx.local_hit)
            continue;
        assert(ctx.local_addr_count >= 1);
        _xor_one_kv(ctx);
    }
}

inline RaceHashSlot * Server::_recover_locate_slot(uint64_t l_kv_addr, uint64_t r_kv_addr, uint32_t remote_sid, uint64_t hash_value, uint64_t hash_prefix, uint8_t hash_fp) {
    KVLogHeader * header = (KVLogHeader *)l_kv_addr;
    uint64_t l_kv_buf_addr = kv_check_buf;
    RaceHashSlot * local_slot = NULL;

    uint64_t local_subtable_addr;
    RaceHashBucket * f_bucket;
    RaceHashBucket * s_bucket;
    uint64_t f_index_value = SubtableFirstIndex(hash_value, RACE_HASH_ADDRESSABLE_BUCKET_NUM); 
    uint64_t s_index_value = SubtableSecondIndex(hash_value, f_index_value, RACE_HASH_ADDRESSABLE_BUCKET_NUM);
    uint64_t f_idx, s_idx;
    uint32_t f_main_idx, s_main_idx;
    RaceHashSlot * slot_arr[4]; 

    if (f_index_value % 2 == 0) 
        f_idx = f_index_value / 2 * 3;
    else
        f_idx = f_index_value / 2 * 3 + 1;
    
    if (s_index_value % 2 == 0)
        s_idx = s_index_value / 2 * 3;
    else 
        s_idx = s_index_value / 2 * 3 + 1;
    
    // get combined bucket off
    f_main_idx = f_index_value % 2;
    s_main_idx = s_index_value % 2;

    assert(race_root->subtable_entry[hash_prefix][0].server_id == my_server_id);
    local_subtable_addr = HashIndexConvert40To64Bits(race_root->subtable_entry[hash_prefix][0].pointer);
    f_bucket = (RaceHashBucket *)(local_subtable_addr + f_idx * sizeof(RaceHashBucket));
    s_bucket = (RaceHashBucket *)(local_subtable_addr + s_idx * sizeof(RaceHashBucket));

    slot_arr[0] = f_bucket[0].slots;
    slot_arr[1] = f_bucket[1].slots;
    slot_arr[2] = s_bucket[0].slots;
    slot_arr[3] = s_bucket[1].slots;

    std::vector<KVRecoverAddr> kv_recover_addr_list;
    std::vector<std::pair<int32_t, int32_t>> kv_idx_list;

    // search all kv pair that finger print matches
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < RACE_HASH_ASSOC_NUM; j ++) {
            if (slot_arr[i][j].atomic.fp == hash_fp && slot_arr[i][j].atomic.offset != 0) {                
                if (header->slot_addr == (uint64_t)&slot_arr[i][j]) {
                    return &slot_arr[i][j];
                } else {
                    KVRecoverAddr cur_kv_addr;
                    cur_kv_addr.r_kv_addr = ConvertOffsetToAddr(slot_arr[i][j].atomic.offset);
                    cur_kv_addr.server_id = slot_arr[i][j].atomic.server_id;
                    cur_kv_addr.l_kv_addr = l_kv_buf_addr;
                    l_kv_buf_addr += define::subblockSize;
                    
                    uint64_t l_block_addr = local_block_map[get_block_addr_with_kv_addr(cur_kv_addr.r_kv_addr, block_size) | cur_kv_addr.server_id];
                    if (l_block_addr != 0) {
                        cur_kv_addr.local_hit = true;
                        cur_kv_addr.need_xor = false;
                        cur_kv_addr.l_kv_addr = l_block_addr + (cur_kv_addr.r_kv_addr - define::baseAddr - HASH_AREA_BORDER) % block_size;
                        check_valid_kv_cnt += 1;

                        uint64_t r_key_addr = cur_kv_addr.l_kv_addr + sizeof(KVLogHeader);
                        uint64_t l_key_addr = l_kv_addr + sizeof(KVLogHeader);
                        KVLogHeader * r_header = (KVLogHeader *)cur_kv_addr.l_kv_addr;
                        KVLogHeader * l_header = (KVLogHeader *)l_kv_addr;
                        if (CheckKey((void *)r_key_addr, r_header->key_length, (void *)l_key_addr, l_header->key_length)) {
                            return &slot_arr[i][j];
                        }
                    } else if (cur_kv_addr.server_id == my_server_id) {
                        cur_kv_addr.local_hit = false;
                        cur_kv_addr.need_xor = true;
                        _recover_prepare_kv_recover(cur_kv_addr, l_kv_buf_addr);
                        check_lost_l_kv_cnt += 1;

                        kv_recover_addr_list.push_back(cur_kv_addr);
                        kv_idx_list.push_back(std::make_pair(i, j));
                    } else {
                        cur_kv_addr.local_hit = false;
                        cur_kv_addr.need_xor = false;
                        cur_kv_addr.local_addr_count = 1;
                        check_lost_r_kv_cnt += 1;

                        kv_recover_addr_list.push_back(cur_kv_addr);
                        kv_idx_list.push_back(std::make_pair(i, j));
                    }
                }
            }
        }
    }

    int k = 0;
    for (int i = 0; i < kv_recover_addr_list.size(); i++) {
        if (kv_recover_addr_list[i].local_hit == false)
            k += kv_recover_addr_list[i].local_addr_count;
    }
    if (k > 0) {
        RdmaOpRegion * ror_list = new RdmaOpRegion[k];
        _fill_recover_kv_ror(ror_list, kv_recover_addr_list);
        network_manager->rdma_read_batches_sync(ror_list, k, nullptr, network_manager->get_ckpt_cq());
        _xor_all_kv(kv_recover_addr_list);
        delete[] ror_list;
    }
    
    int match_id = -1;
    for (size_t i = 0; i < kv_recover_addr_list.size(); i ++) {
        uint64_t r_key_addr = kv_recover_addr_list[i].l_kv_addr + sizeof(KVLogHeader);
        uint64_t l_key_addr = l_kv_addr + sizeof(KVLogHeader);
        KVLogHeader * r_header = (KVLogHeader *)kv_recover_addr_list[i].l_kv_addr;
        KVLogHeader * l_header = (KVLogHeader *)l_kv_addr;
        if (CheckKey((void *)r_key_addr, r_header->key_length, (void *)l_key_addr, l_header->key_length)) {
            match_id = i;
            break;
        }
    }

    if (match_id != -1) {
        int i = kv_idx_list[match_id].first;
        int j = kv_idx_list[match_id].second;
        local_slot = &slot_arr[i][j];
    } else {
        // TODO: if the old slot of this key is not found, allocate an empty slot in a temporary memory area, and insert it back into the index after checking all KVs
        printf("not find old slot\n");
    }
    return local_slot;
}

int Server::_recover_check_one_kv(uint64_t l_kv_addr, uint64_t r_kv_addr, uint32_t remote_sid) {
    KVLogHeader * header = (KVLogHeader *)l_kv_addr;
    if (header->slot_sid != my_server_id)
        return 0;
    
    uint64_t key_addr = l_kv_addr + sizeof(KVLogHeader);
    uint64_t hash_value = VariableLengthHash((void *)key_addr, header->key_length, 0);
    uint64_t hash_prefix = (hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & RACE_HASH_MASK(race_root->global_depth);
    uint8_t  hash_fp = HashIndexComputeFp(hash_value); 

    RaceHashSlot * race_slot = _recover_locate_slot(l_kv_addr, r_kv_addr, remote_sid, hash_value, hash_prefix, hash_fp);
    assert(race_slot != NULL);
    if ((header->version.log.epoch > race_slot->slot_meta.epoch) ||
        (header->version.log.epoch == race_slot->slot_meta.epoch && 
        header->version.log.version > race_slot->atomic.version)) {
        // repair the index slot
        uint8_t slot_fp = race_slot->atomic.fp;
        uint8_t slot_version = race_slot->atomic.version;
        assert((slot_version == 0) || (slot_fp == hash_fp));
        race_slot->atomic.fp = hash_fp;
        race_slot->atomic.version = header->version.log.version;
        race_slot->atomic.offset = ConvertAddrToOffset(r_kv_addr);
        race_slot->atomic.server_id = remote_sid;
        race_slot->slot_meta.kv_len = 1;
        race_slot->slot_meta.epoch = header->version.log.epoch;
    }
}

int Server::_recover_check_one_block(MMBlockRecoverContext & ctx) {
    uint64_t l_blk_addr, r_blk_addr, remote_sid;
    if (ctx.is_remote) {
        l_blk_addr = ctx.local_addr[0];
        r_blk_addr = ctx.primary_addr;
        remote_sid = ctx.remote_server_id[0];
    } else {
        l_blk_addr = r_blk_addr = ctx.primary_addr;
        remote_sid = my_server_id;
    }
    for (int i = bmap_subblock_num; i < subblock_num; i++) {
        uint64_t l_kv_addr = l_blk_addr + i * define::subblockSize;
        uint64_t r_kv_addr = r_blk_addr + i * define::subblockSize;
        KVLogHeader * header = (KVLogHeader *)l_kv_addr;
        if (header_is_valid(header)) {
            _recover_check_one_kv(l_kv_addr, r_kv_addr, remote_sid);
            total_check_count++;
        }
    }
}

int Server::_recover_check_all() {
    total_check_count = 0;
    kv_check_buf = _get_new_recover_block_addr();
    for (int i = 0; i < local_block_list.size(); i++) {
        _recover_check_one_block(local_block_list[i]);
    }
    for (int i = 0; i < remote_block_list.size(); i++) {
        _recover_check_one_block(remote_block_list[i]);
    }
    return 0;
}

void Server::_init_memory() {
    data_raw = (void *)createMemoryRegion(define::baseAddr, define::serverMemorySize);
    assert((uint64_t)data_raw == define::baseAddr);
    
    int access_flag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    ib_mr = ibv_reg_mr(rdma_ctx.pd, (void *)data_raw, define::serverMemorySize, access_flag);
    if (ib_mr == NULL) {
        printf("Failed to register mr, errno: %d\n", errno);
    }
}

void Server::_init_layout() {
    assert(define::memoryNodeNum > 2);
    slab_num = define::memoryNodeNum;
    
    uint64_t memblock_area_total_len = round_down(define::serverMemorySize - HASH_AREA_BORDER, slab_num * block_size);
    uint64_t memblock_area_valid_len = memblock_area_total_len / slab_num * (slab_num - 2);
    uint64_t memblock_area_parit_len = memblock_area_total_len / slab_num * 2;
    slab_size = memblock_area_total_len / slab_num;

    client_meta_area_off = 0;
    client_meta_area_len = META_AREA_LEN;
    client_hash_area_off = META_AREA_BORDER;
    client_hash_area_len = HASH_AREA_LEN * INDEX_REPLICA;
    client_kv_area_off   = HASH_AREA_BORDER;
    client_kv_pari_off   = HASH_AREA_BORDER + memblock_area_valid_len;
    client_kv_area_len   = memblock_area_valid_len;

    num_blocks          = client_kv_area_len / block_size;
    num_parity_blocks   = memblock_area_parit_len / block_size;

    for (int i = 0; i < 3; i++) {
        hash_ckpt_addr[i] = define::baseAddr + META_AREA_BORDER + (uint64_t)i * HASH_AREA_LEN;
    }
    for (int i = 0; i < 3; i++) {
        hash_delta_addr[i] = define::baseAddr + META_AREA_BORDER + (uint64_t)3 * HASH_AREA_LEN + (uint64_t)i * define::ckptDeltaMaxSize;
    }
    hash_local_buf[0] = mmap(NULL, HASH_AREA_LEN * 3, PROT_READ | PROT_WRITE, 
                            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    assert(hash_local_buf[0] != MAP_FAILED);
    hash_local_buf[1] = (void *)((uint64_t)hash_local_buf[0] + HASH_AREA_LEN);
    hash_local_buf[2] = (void *)((uint64_t)hash_local_buf[1] + HASH_AREA_LEN);
}

void Server::_init_block() {
    uint64_t kv_area_addr = define::baseAddr + client_kv_area_off;
    uint32_t block_cnt = 0;

    // clear last queue
    while(!allocable_blocks.empty()) {
        allocable_blocks.pop();
    }

    while (block_cnt < this->num_blocks) {
        allocable_blocks.push(kv_area_addr);

        MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr(kv_area_addr, my_server_id, block_size);
        block_meta->block_role = FREE_BLOCK;
        block_meta->normal.index_ver = 0;
        block_meta->normal.xor_idx = 0xFF;

        kv_area_addr += block_size;
        block_cnt ++;
    }

    kv_area_addr = define::baseAddr + client_kv_pari_off;
    block_cnt = 0;

    while (block_cnt < this->num_parity_blocks) {
        MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr(kv_area_addr, my_server_id, block_size);
        block_meta->block_role = PARITY_BLOCK;
        std::fill(block_meta->parity.delta_addr, block_meta->parity.delta_addr + 3, 0);
        block_meta->parity.xor_map = 0;

        kv_area_addr += block_size;
        block_cnt ++;
    }
}

void Server::_init_ckpt() {
    index_ver = 0;
    crash_happen = 0;
    network_manager->server_set_index_ver(0);
}

void Server::_init_rs_code() {
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
        ret = posix_memalign((void **)&temp_buff_list[i], 64, block_size);   assert(temp_buff_list[i] != NULL);
        assert(ret == 0);
        memset(temp_buff_list[i], 0, block_size);
    }
}

uint64_t Server::_mm_alloc() {
    if (allocable_blocks.size() == 0) {
        printf("run out of memory block.\n");
        return 0;
    }

    uint64_t ret_addr = allocable_blocks.front();
    MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr(ret_addr, my_server_id, block_size);
    block_meta->block_role = DATA_BLOCK;
    block_meta->normal.index_ver = define::unXorSeal;
    block_meta->normal.xor_idx = 0xFF;

    allocable_blocks.pop();
    allocated_blocks[ret_addr] = true;
    return ret_addr;
}

int Server::_mm_free(uint64_t st_addr) {
    if (allocated_blocks[st_addr] != true) {
        printf("[DEBUG] mm_free a wrong addr %lx\n", st_addr);
        return -1;
    }

    MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr(st_addr, my_server_id, block_size);
    block_meta->block_role = FREE_BLOCK;
    block_meta->normal.index_ver = 0;
    block_meta->normal.xor_idx = 0xFF;

    memset((void *) st_addr, 0, block_size);
    allocated_blocks[st_addr] = false;
    allocable_blocks.push(st_addr);
    return 0;
}

uint64_t Server::_mm_alloc_subtable() {
    int ret = 0;
    uint64_t subtable_st_addr = define::baseAddr + client_hash_area_off + roundup_256(ROOT_RES_LEN);
    for (size_t i = 0; i < subtable_alloc_map.size(); i ++) {
        if (subtable_alloc_map[i] == 0) {
            subtable_alloc_map[i] = 1;

            uint64_t cur_subtable_addr = subtable_st_addr + i * roundup_256(SUBTABLE_LEN);
            for (int j = 0; j < RACE_HASH_SUBTABLE_BUCKET_NUM; j ++) {
                RaceHashBucket * bucket = (RaceHashBucket *)cur_subtable_addr + j;
                bucket->local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
                bucket->prefix = i;
            }

           return cur_subtable_addr;
        }
    }
    return 0;
}

uint32_t Server::_get_rkey() {
    return ib_mr->rkey;
}

int Server::_get_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = define::baseAddr;
    mr_info->rkey = ib_mr->rkey;
    return 0;
}

int Server::_init_root(void * root_addr) {
    RaceHashRoot * root = (RaceHashRoot *)root_addr;
    root->index_ver = 0;
    root->global_depth = RACE_HASH_GLOBAL_DEPTH;
    root->init_local_depth = RACE_HASH_INIT_LOCAL_DEPTH;
    root->max_global_depth = RACE_HASH_MAX_GLOBAL_DEPTH;
    root->prefix_num = 1 << RACE_HASH_MAX_GLOBAL_DEPTH;
    root->subtable_res_num = root->prefix_num;
    root->subtable_init_num = RACE_HASH_INIT_SUBTABLE_NUM;
    root->subtable_hash_range = RACE_HASH_ADDRESSABLE_BUCKET_NUM;
    root->subtable_bucket_num = RACE_HASH_SUBTABLE_BUCKET_NUM;
    root->seed = rand();
    root->root_offset = client_hash_area_off;
    root->subtable_offset = root->root_offset + roundup_256(ROOT_RES_LEN);
    root->kv_offset = client_kv_area_off;
    root->kv_len = client_kv_area_len;
    root->lock = 0;
    return 0;
}

int Server::_init_subtable(void * subtable_addr) {
    uint64_t max_subtables = (define::baseAddr + client_hash_area_off + client_hash_area_len - (uint64_t)subtable_addr) / roundup_256(SUBTABLE_LEN);

    subtable_alloc_map.resize(max_subtables);
    for (int i = 0; i < max_subtables; i ++) {
        uint64_t cur_subtable_addr = (uint64_t)subtable_addr + i * roundup_256(SUBTABLE_LEN);
        subtable_alloc_map[i] = 0;
        for (int j = 0; j < RACE_HASH_SUBTABLE_BUCKET_NUM; j ++) {
            RaceHashBucket * bucket = (RaceHashBucket *)cur_subtable_addr + j;
            bucket->local_depth = 0;
            bucket->prefix = 0;
        }
    }

    return 0;
}

int Server::_init_hashtable() {
    uint64_t root_addr = define::baseAddr + client_hash_area_off;
    uint64_t subtable_st_addr = _get_subtable_st_addr();
    _init_root((void *)(root_addr));
    _init_subtable((void *)(subtable_st_addr));
    return 0;
}

uint64_t Server::_get_kv_area_addr() {
    return client_kv_area_off + define::baseAddr;
}

uint64_t Server::_get_subtable_st_addr() {
    return client_hash_area_off + define::baseAddr + roundup_256(ROOT_RES_LEN);
}

// get the DATA block's index in xor map
uint32_t Server::_get_index_in_xor_map(uint64_t pr_addr) {
    uint64_t pr_off = pr_addr - define::baseAddr;
    uint16_t ret = (slab_num - 3) - ((pr_off - client_kv_area_off) / slab_size);
    assert(ret < (slab_num - 2));
    return ret;
}

int Server::_ckpt_set_index_ver(uint64_t index_ver) {
    uint64_t root_addr = define::baseAddr + client_hash_area_off;
    RaceHashRoot * root = (RaceHashRoot *)root_addr;
    root->index_ver = index_ver;
    return 0;
}

int Server::_ckpt_send_index_simple(const struct KVMsg * request) {
    RdmaOpRegion write_ror[define::replicaNum - 1];
    _fill_write_index_ror(write_ror, hash_ckpt_addr[0], HASH_AREA_LEN);
    network_manager->rdma_write_batches_sync(write_ror, define::replicaNum - 1, nullptr, network_manager->get_ckpt_cq());

    // reply
    if (request != NULL) {
        struct KVMsg * reply = _prepare_reply(request);
        reply->type = REP_CKPT_SEND;
        network_manager->send_reply(reply);
    }
    return 0;
}

int Server::_ckpt_send_index(const struct KVMsg * request) {
    uint64_t index_size = HASH_AREA_LEN;
    uint64_t index_addr = hash_ckpt_addr[0];
    uint64_t index_ckpt_addr = (uint64_t)hash_local_buf[0];
    uint64_t encode_buf_addr = (uint64_t)hash_local_buf[1];
    uint64_t * compress_size_addr = (uint64_t *)hash_delta_addr[0];
    uint64_t compress_buf_addr = hash_delta_addr[0] + 32;

    struct timeval perf_clock_1, perf_clock_2, perf_clock_3;
    uint64_t time_cost_1, time_cost_2; 

    // checkpoint sending
    gettimeofday(&perf_clock_1, NULL);
    avx_memcpy_128_ckpt(index_addr, index_ckpt_addr, encode_buf_addr, index_size);
    gettimeofday(&perf_clock_2, NULL);
    int compress_size = LZ4_compress_fast((const char *)encode_buf_addr, (char *)compress_buf_addr, index_size, (index_size - 32), 1);
    gettimeofday(&perf_clock_3, NULL);
    assert(compress_size < (index_size - 32));
    *compress_size_addr = compress_size;
    time_cost_1 = time_spent_us(&perf_clock_1, &perf_clock_2);
    time_cost_2 = time_spent_us(&perf_clock_2, &perf_clock_3);

    if (HASH_AREA_LEN > (128ULL * 1024 * 1024) && my_server_id == 0) {
        printf("\n\n");
        printf("[CHECKPOINT] Index Size(B) %lu\n", index_size);
        printf("[CHECKPOINT] Compress Size(B) %d\n", compress_size);
        printf("[CHECKPOINT] Copy&XOR Time(us) %lu\n", time_cost_1);
        printf("[CHECKPOINT] Compress Time(us) %lu\n", time_cost_2);
    }

    // write delta
    RdmaOpRegion write_ror[define::replicaNum - 1];
    _fill_write_index_ckpt_ror(write_ror, (uint64_t)compress_size_addr, (compress_size + 32));
    network_manager->rdma_write_batches_sync(write_ror, define::replicaNum - 1, nullptr, network_manager->get_ckpt_cq());

    // notify checkpoint recving
    this->hash_ckpt_recv = 0;
    struct KVMsg * ckpt_recv_request = _prepare_request();
    ckpt_recv_request->type = REQ_CKPT_RECV;
    uint32_t server_id_list[define::replicaNum - 1];
    for (int i = 1; i <= define::replicaNum - 1; i++)
        server_id_list[i - 1] = (my_server_id + i) % define::memoryNodeNum;
    network_manager->send_broad_request(ckpt_recv_request, server_id_list, define::replicaNum - 1);

    std::unique_lock<std::mutex> lock(ckpt_send_mutex);
    ckpt_send_cv.wait(lock, [this] { return this->hash_ckpt_recv == define::replicaNum - 1; });
    lock.unlock();

    // reply
    if (request != NULL) {
        struct KVMsg * reply = _prepare_reply(request);
        reply->type = REP_CKPT_SEND;
        network_manager->send_reply(reply);
    }
    return 0;
}

int Server::_ckpt_recv_index(const struct KVMsg * request) {
    uint32_t server_id = request->nd_id;
    uint64_t index_size = HASH_AREA_LEN;
    uint64_t index_delta_compress_addr, index_delta_decompress_addr, index_replica_addr;
    uint64_t compress_size;
    if (my_server_id == (server_id + 1) % define::memoryNodeNum) {
        index_delta_compress_addr = hash_delta_addr[1] + 32;
        index_delta_decompress_addr = (uint64_t)hash_local_buf[2];
        index_replica_addr = hash_ckpt_addr[1];
        compress_size = *((uint64_t *)hash_delta_addr[1]);
    } else {
        assert(my_server_id == (server_id + 2) % define::memoryNodeNum);
        index_delta_compress_addr = hash_delta_addr[2] + 32;
        index_delta_decompress_addr = (uint64_t)hash_local_buf[2];
        index_replica_addr = hash_ckpt_addr[2];
        compress_size = *((uint64_t *)hash_delta_addr[2]);
    }

    struct timeval perf_clock_1, perf_clock_2, perf_clock_3;
    uint64_t time_cost_1, time_cost_2; 

    gettimeofday(&perf_clock_1, NULL);
    int decompressed_size = LZ4_decompress_safe((const char *)index_delta_compress_addr, 
                                                (char *)index_delta_decompress_addr, compress_size, index_size);
    assert(decompressed_size == index_size);
    gettimeofday(&perf_clock_2, NULL);
    avx_xor_buffers(index_replica_addr, index_delta_decompress_addr, index_replica_addr, index_size);
    gettimeofday(&perf_clock_3, NULL);
    time_cost_1 = time_spent_us(&perf_clock_1, &perf_clock_2);
    time_cost_2 = time_spent_us(&perf_clock_2, &perf_clock_3);
    if (HASH_AREA_LEN > (128ULL * 1024 * 1024) && my_server_id == 0) {
        printf("[CHECKPOINT] Decompress Time(us) %lu\n", time_cost_1);
        printf("[CHECKPOINT] XOR Time(us) %lu\n", time_cost_2);
    }

    // reply
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_CKPT_RECV;
    network_manager->send_reply(reply);
    return 0;
}

void Server::_fill_write_index_ror(RdmaOpRegion * ror, uint64_t local_addr, uint64_t write_size) {
    for (int i = 1; i <= define::replicaNum - 1; i++) {
        ror[i - 1].remote_addr = hash_ckpt_addr[i];
        ror[i - 1].remote_sid = (my_server_id + i) % define::memoryNodeNum;
        ror[i - 1].local_addr = local_addr;
        ror[i - 1].size = write_size;
        ror[i - 1].op_code = IBV_WR_RDMA_WRITE;
    }
}

void Server::_fill_write_index_ckpt_ror(RdmaOpRegion * ror, uint64_t local_addr, uint64_t write_size) {
    for (int i = 1; i <= define::replicaNum - 1; i++) {
        ror[i - 1].remote_addr = hash_delta_addr[i];
        ror[i - 1].remote_sid = (my_server_id + i) % define::memoryNodeNum;
        ror[i - 1].local_addr = local_addr;
        ror[i - 1].size = write_size;
        ror[i - 1].op_code = IBV_WR_RDMA_WRITE;
    }
}

struct KVMsg * Server::_prepare_reply(const struct KVMsg * request) {
    send_pool_lock.lock();
    struct KVMsg * reply = (struct KVMsg *)network_manager->rpc_get_send_pool();
    send_pool_lock.unlock();
    reply->nd_id = request->nd_id;
    reply->th_id = request->th_id;
    reply->co_id = request->co_id;
    return reply;
}

struct KVMsg * Server::_prepare_request() {
    send_pool_lock.lock();
    struct KVMsg * request = (struct KVMsg *)network_manager->rpc_get_send_pool();
    send_pool_lock.unlock();
    request->nd_id = my_server_id;
    request->th_id = 0;
    request->co_id = 0;
    return request;
}

int Server::_ckpt_check_prev() {
    struct timeval last, curr;
    gettimeofday(&last, NULL);
    for (int i = 0; i < define::memoryNodeNum; i++) {
        if (i == my_server_id) 
            continue;
        uint64_t * server_index_ver;
        network_manager->server_get_index_ver(i, &server_index_ver);
            
        while (*server_index_ver != this->index_ver) {
            gettimeofday(&curr, NULL);
            if (time_spent_ms(&last, &curr) >= 2000) {
                printf("[CHECKPOINT WARNING] crash happened %d\n", i);
                crash_happen = 1;
                return -1;
            }
            network_manager->server_get_index_ver(i, &server_index_ver);
        }
    }
    return 0;
}

// leader MN will execute this function
int Server::server_ckpt_iteration_start() {
    assert(my_server_id == 0);
    if (crash_happen != 0)
        return -1;
    
    this->hash_ckpt_send = 0;
    struct KVMsg * request = _prepare_request();
    request->type = REQ_CKPT_SEND;
    request->body.ckpt_info.new_time = this->index_ver + 1;
    uint32_t server_id_list[define::memoryNodeNum - 1];
    for (int i = 0 ; i < define::memoryNodeNum - 1; i++)
        server_id_list[i] = i + 1;
    network_manager->send_broad_request(request, server_id_list, define::memoryNodeNum - 1);
    
    return 0;
}

int Server::server_ckpt_iteration_wait() {
    std::unique_lock<std::mutex> lock(ckpt_send_mutex);
    ckpt_send_cv.wait(lock, [this] { return this->hash_ckpt_send == define::memoryNodeNum - 1; });
    lock.unlock();
}

int Server::server_on_connect(const struct KVMsg * request) {
    int rc = 0;
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_CONNECT;

    // printf("receive connect request from node %d\n", request->nd_id);
    rc = network_manager->server_on_connect_new_qp(request, &(reply->body.conn_info.qp_info));
    // assert(rc == 0);

    rc = _get_mr_info(&(reply->body.conn_info.gc_info));
    // assert(rc == 0);

    rc = network_manager->send_reply(reply);
    // assert(rc == 0);
    
    uint32_t unique_id = get_unique_id(request->nd_id, request->th_id); 
    rc = network_manager->server_on_connect_connect_qp(unique_id, &(reply->body.conn_info.qp_info), &(request->body.conn_info.qp_info));
    return 0;
}

int Server::server_on_alloc(const struct KVMsg * request) {
    uint64_t alloc_addr = _mm_alloc();

    // here simulate copying the origin DATA block
    if (request->body.mr_info.addr == 0x1234) {
        uint64_t block_addr = define::baseAddr + client_kv_pari_off;
        memcpy((void *)block_addr, (void *)alloc_addr, block_size);
    }

    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_ALLOC;
    reply->body.mr_info.rkey = _get_rkey();
    if (alloc_addr != 0) {
        reply->body.mr_info.addr = alloc_addr;
    } else {
        printf("server no space\n");
        reply->body.mr_info.addr = 0;
    }
    network_manager->send_reply(reply);

    return 0;
}

int Server::server_on_alloc_subtable(const struct KVMsg * request) {
    uint64_t subtable_addr = _mm_alloc_subtable();
    if (subtable_addr == 0) {
        printf("server subtable no space\n");
    }

    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_ALLOC_SUBTABLE;
    reply->body.mr_info.addr = subtable_addr;
    reply->body.mr_info.rkey = _get_rkey();

    network_manager->send_reply(reply);

    return 0;
}

int Server::server_on_alloc_delta(const struct KVMsg * request) {
    uint64_t alloc_addr = _mm_alloc();
    assert(alloc_addr != 0);

    MrInfo primar_mr_info;
    MrInfo parity_mr_info;
    primar_mr_info.addr = request->body.two_mr_info.prim_addr;
    primar_mr_info.rkey = request->body.two_mr_info.prim_rkey;
    parity_mr_info.addr = request->body.two_mr_info.pari_addr;
    parity_mr_info.rkey = request->body.two_mr_info.pari_rkey;

    // set delta block meta
    uint32_t xor_idx = _get_index_in_xor_map(primar_mr_info.addr);
    MMBlockMeta * block_meta = (MMBlockMeta *)get_meta_addr(alloc_addr, my_server_id, block_size);
    block_meta->block_role = DELTA_BLOCK;
    block_meta->normal.index_ver = define::unXorSeal;
    block_meta->normal.xor_idx = xor_idx;
    
    // set parity block meta
    MMBlockMeta * parity_meta = (MMBlockMeta *)get_meta_addr(parity_mr_info.addr, my_server_id, block_size);
    assert(parity_meta->block_role == PARITY_BLOCK);
    parity_meta->parity.delta_addr[xor_idx] = alloc_addr;

    // reply
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_ALLOC_DELTA;
    reply->body.mr_info.rkey = _get_rkey();
    if (alloc_addr != 0) {
        reply->body.mr_info.addr = alloc_addr;
    } else {
        printf("Server no space.\n");
        reply->body.mr_info.addr = 0;
    }

    // printf("server alloc delta blocks, left: %u\n", allocable_blocks.size());
    network_manager->send_reply(reply);
    
    return 0;
}

int Server::server_on_seal(const struct KVMsg * request) {
    // reply
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_SEAL;
    network_manager->send_reply(reply);
    return 0;
}

// can only xor memory block
int Server::server_on_xor(const struct KVMsg * request) {
    assert(request->type == REQ_XOR);
    uint64_t delta_block_addr = request->body.xor_info.delta_addr;
    uint64_t parit_block_addr = request->body.xor_info.parity_addr;
    size_t xor_size = block_size;
    
    MMBlockMeta * delt_meta = (MMBlockMeta *)get_meta_addr(delta_block_addr, my_server_id, block_size);
    MMBlockMeta * pari_meta = (MMBlockMeta *)get_meta_addr(parit_block_addr, my_server_id, block_size);
    assert(delt_meta->block_role == DELTA_BLOCK);
    assert(pari_meta->block_role == PARITY_BLOCK);

    uint8_t xor_idx = delt_meta->normal.xor_idx; 
    assert(xor_idx < 3);

    // reply
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_XOR;
    reply->body.xor_info.index_ver = this->index_ver;
    network_manager->send_reply(reply);

    if (define::codeMethod == RS_CODE)
        _rs_update(delta_block_addr, parit_block_addr, xor_size, xor_idx);  // use RS code
    else
        xor_add_buffers(delta_block_addr, parit_block_addr, xor_size);      // use XOR code
    
    pari_meta->parity.xor_map |= (1 << xor_idx);
    pari_meta->parity.delta_addr[xor_idx] = 0;

    _mm_free(request->body.xor_info.delta_addr);
    return 0;
}

int Server::server_on_ckpt_send(const struct KVMsg * request) {
    if (!is_recovery) {
        // check if from MN self
        if (request == NULL) {
#ifdef ENABLE_SEND_SIMPLE
            _ckpt_send_index_simple(request);
#else
            _ckpt_send_index(request);
#endif
            this->index_ver += 1;
            _ckpt_set_index_ver(this->index_ver);
            return 0;
        }

        uint64_t new_time = request->body.ckpt_info.new_time;
        if (new_time != (this->index_ver + 1)) {
            printf("[CHECKPOINT WARNING] new_time %lu index_ver %lu\n", new_time, this->index_ver);
        }

#ifdef ENABLE_SEND_SIMPLE
        _ckpt_send_index_simple(request);
#else
        _ckpt_send_index(request);
#endif
        this->index_ver = new_time;
        _ckpt_set_index_ver(this->index_ver);
    }
    return 0;
}

int Server::server_on_ckpt_recv(const struct KVMsg * request) {
    if (!is_recovery && request->type == REQ_CKPT_RECV) {
        _ckpt_recv_index(request);
    }
    return 0;
}

int Server::server_on_ckpt_enable(const struct KVMsg * request) {
    assert(my_server_id == 0);
    this->ckpt_start = true;
    // reply
    struct KVMsg * reply = _prepare_reply(request);
    reply->type = REP_CKPT_ENABLE;
    network_manager->send_reply(reply);
    return 0;
}

void thread_xor(boost::lockfree::queue<KVMsg> & message_queue, Server * server) {
    stick_this_thread_to_core(define::xorCoreID);

    while (!server->need_stop) {
        std::unique_lock<std::mutex> lock(xor_mutex);
        xor_cv.wait(lock, [&message_queue] { return !message_queue.empty(); });
        lock.unlock();

        KVMsg request;
        while (message_queue.pop(request)) {
            server->server_on_xor(&request);
        }
    }
}

void thread_ckpt_send(boost::lockfree::queue<KVMsg> & message_queue, Server * server) {
    stick_this_thread_to_core(define::ckptSendCoreID);
    struct KVMsg request;
    auto last_ckpt_time = std::chrono::high_resolution_clock::now();
    auto ckpt_interval = std::chrono::milliseconds(server->ckpt_interval);
    while (!server->need_stop) {
        if (server->my_server_id == 0) {
            if (server->ckpt_start) {
                // boost::this_thread::sleep_for(boost::chrono::milliseconds(server->ckpt_interval));
                auto curr_ckpt_time = std::chrono::high_resolution_clock::now();
                if (std::chrono::duration_cast<std::chrono::milliseconds>(curr_ckpt_time - last_ckpt_time) >= ckpt_interval) {
                    last_ckpt_time = curr_ckpt_time;
                    server->server_ckpt_iteration_start();
                    if (server->crash_happen == 0) {
                        server->server_on_ckpt_send(NULL);
                    }
                    server->server_ckpt_iteration_wait();
                }
                
                // auto end_time = std::chrono::high_resolution_clock::now();
                // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                // printf("[CHECKPOINT] One iteration spent: %ld ms.\n", duration.count());
            }
        } else {
            std::unique_lock<std::mutex> lock(ckpt_send_mutex);
            ckpt_send_cv.wait(lock, [&message_queue] { return !message_queue.empty(); });
            lock.unlock();

            KVMsg request;
            while (message_queue.pop(request)) {
                server->server_on_ckpt_send(&request);
            }
        }
    }
}

void thread_ckpt_recv(boost::lockfree::queue<KVMsg> & message_queue, Server * server) {
    stick_this_thread_to_core(define::ckptRecvCoreID);
    struct KVMsg request;
    while (!server->need_stop) {
        std::unique_lock<std::mutex> lock(ckpt_recv_mutex);
        ckpt_recv_cv.wait(lock, [&message_queue] { return !message_queue.empty(); });
        lock.unlock();

        KVMsg request;
        while (message_queue.pop(request)) {
            server->server_on_ckpt_recv(&request);
        }
    }
}

void * Server::thread_main() {
    int rc = 0;
    stick_this_thread_to_core(define::mainCoreID);

    boost::lockfree::queue<KVMsg> xor_queue(512);
    boost::thread xor_thread(boost::bind(thread_xor, boost::ref(xor_queue), this));
    boost::lockfree::queue<KVMsg> ckpt_send_queue(512);
    boost::thread ckpt_send_thread(boost::bind(thread_ckpt_send, boost::ref(ckpt_send_queue), this));
    boost::lockfree::queue<KVMsg> ckpt_recv_queue(512);
    boost::thread ckpt_recv_thread(boost::bind(thread_ckpt_recv, boost::ref(ckpt_recv_queue), this));
    
    time_t last_ckpt_time = time(NULL);
    ibv_wc wc;
    while (!need_stop) {
#ifdef ENABLE_RPC_BLOCK
        rc = network_manager->poll_cq_once_block(&wc, network_manager->get_rpc_recv_cq());
        if (rc <= 0)
            continue;
#else
        rc = network_manager->poll_cq_once_sync(&wc, network_manager->get_rpc_recv_cq());
        assert(rc == 1);
#endif
        if (need_stop)
            break;
        assert(wc.opcode == IBV_WC_RECV);
        KVMsg *request = (KVMsg *)network_manager->rpc_get_message();
        if (request->type == REQ_CONNECT) {
            rc = server_on_connect(request);
            assert(rc == 0);
        } else if (request->type == REQ_ALLOC_SUBTABLE) {
            rc = server_on_alloc_subtable(request);
            assert(rc == 0);
        } else if (request->type == REQ_ALLOC_DELTA) {
            rc = server_on_alloc_delta(request);
            assert(rc == 0);
        } else if (request->type == REQ_SEAL) {
            rc = server_on_seal(request);
            assert(rc == 0);
        } else if (request->type == REQ_XOR) {
            std::unique_lock<std::mutex> lock(xor_mutex);            
            xor_queue.push(*request);
            xor_cv.notify_all();
        } else if (request->type == REQ_CKPT_SEND) {
            std::unique_lock<std::mutex> lock(ckpt_send_mutex);
            ckpt_send_queue.push(*request);
            ckpt_send_cv.notify_all();
        } else if (request->type == REP_CKPT_SEND) {
            std::unique_lock<std::mutex> lock(ckpt_send_mutex);
            this->hash_ckpt_send += 1;
            ckpt_send_cv.notify_all();  // TODO:
        } else if (request->type == REQ_CKPT_RECV) {
            std::unique_lock<std::mutex> lock(ckpt_recv_mutex);
            ckpt_recv_queue.push(*request);
            ckpt_recv_cv.notify_all();
        } else if (request->type == REP_CKPT_RECV) {
            std::unique_lock<std::mutex> lock(ckpt_send_mutex);
            this->hash_ckpt_recv += 1;
            ckpt_send_cv.notify_all();
        } else if (request->type == REQ_CKPT_ENABLE) {
            rc = server_on_ckpt_enable(request);
            assert(rc == 0);
        } else {
            assert(request->type == REQ_ALLOC);
            rc = server_on_alloc(request);
            assert(rc == 0);
        } 
    }
    return NULL;
}

void Server::stop() {
    need_stop = 1;
}