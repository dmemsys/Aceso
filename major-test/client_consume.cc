// specifically for memory consumption test
#include "Common.h"
#include "KvUtils.h"
#include "AddrCache.h"
#include "Client.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <string>
#include <fstream>
#include <random>
#include <chrono>

int my_server_id;           // parsed args
int cn_num;
int thread_num;
int coro_num;
std::string workload_name;  // parsed args
AddrCache * addr_cache;
Client * all_clients[define::maxClientNum];
std::thread th[MAX_APP_THREAD];
std::atomic<int32_t> warmup_cnt{0};

void thread_consume(Client *client, std::string workload_name, uint64_t run_count) {
    assert (is_micro_test(workload_name) == true);
    int32_t num_ops = define::microLoadKeyNum;
    std::string op_type = micro_get_op_type(workload_name);

    client->load_kv_requests(0, num_ops, get_load_path(workload_name), op_type);
    client->init_kv_req_space(0, 0, client->req_local_num, nullptr);

    printf("Consume phase start.\n");
    
    int ret = 0;
    bool should_stop = false;
    uint32_t num_failed = 0;
    void * search_addr = NULL;

    auto start_time = std::chrono::high_resolution_clock::now();
    std::unordered_map<std::string, bool> inserted_key_map;
    for (uint32_t i = 0; i < run_count; i++) {
        uint32_t idx = i % client->req_local_num;
        KVReqCtx * ctx = &client->kv_req_ctx_list[idx];
        ctx->coro_id = 0;
        ctx->should_stop = &should_stop;

        switch (ctx->req_type) {
            case KV_OP_SEARCH:
                search_addr = client->kv_search(ctx);
                if (search_addr == NULL) {
                    num_failed ++;
                }
                break;
            case KV_OP_INSERT:
                if (inserted_key_map[ctx->key_str] == true) {
                    char * modify = (char *)((uint64_t)(ctx->kv_info->l_addr) + sizeof(KVLogHeader));
                    modify[4]++;
                    ctx->key_str[4]++;
                }
                do {
                    ret = client->kv_insert(ctx);
                } while (ret == KV_OPS_FAIL_REDO);
                if (ret == KV_OPS_FAIL_RETURN) {
                    num_failed++;
                } else {
                    inserted_key_map[ctx->key_str] = true;
                }
                break;
            case KV_OP_UPDATE:
                client->kv_update(ctx);
                break;
            case KV_OP_DELETE:
                assert(0);
                break;
            default:
                client->kv_search(ctx);
                break;
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    printf("Consume phase ends (failed %d).\n", num_failed);
    printf("Time spent: %ld ms.\n", duration.count());
    return;
}

void thread_run(int my_thread_id, volatile bool *should_stop) {
    stick_this_thread_to_core(my_thread_id + 2);

    Client * client = new Client(my_server_id, my_thread_id, (cn_num * thread_num), addr_cache);
    all_clients[my_thread_id] = client;

    printf("Client %d start.\n", my_thread_id);

    assert (is_micro_test(workload_name) == true);
    // insert load
    thread_consume(client, "insert", 50000);
    client->client_barrier("load-load-complete");
    printf("Client %d passed load barrier.\n", my_thread_id);

    // start ycsb test
    warmup_cnt.fetch_add(1);
    thread_consume(client, "update", 250000);
    printf("Client %d exit.\n", my_thread_id);
}

void parse_args(int argc, char *argv[]) {
    if (argc != 5) {
        printf("Usage: ./perf_test workload_name cn_num thread_num coro_num\n");
        exit(-1);
    }
    workload_name = std::string(argv[1]);
    cn_num = atoi(argv[2]);
    thread_num = atoi(argv[3]); assert(thread_num <= define::maxClientNum);
    coro_num = atoi(argv[4]);
    printf("server_id: %d, workload_name: %s\n", my_server_id, workload_name.c_str());
}

int main(int argc, char *argv[]) {
    parse_args(argc, argv);
    stick_this_thread_to_core(0);

    GlobalConfig config;
    int ret = load_config("./config.json", &config); assert(ret == 0);
    my_server_id = config.server_id;

    addr_cache = new AddrCache();
    
    volatile bool should_stop = false;
    for (int i = 0; i < thread_num; i ++) {
        th[i] = std::thread(std::bind(thread_run, i, &should_stop));
    }

    timespec exp_st, exp_ed;
    uint64_t total_finish = 0, total_failed = 0, total_modify = 0, total_cas = 0;
    uint64_t total_search[5];
    std::fill(total_search, total_search + 5, 0);
    uint64_t total_hit = 0, total_mis = 0, total_cur = 0, total_raw = 0;

    while (warmup_cnt.load() != thread_num)
        ;

    double run_time = 20;
    
    clock_gettime(CLOCK_REALTIME, &exp_st);
    usleep(run_time * 1000000);
    should_stop = true;
    clock_gettime(CLOCK_REALTIME, &exp_ed);

    for (int i = 0; i < thread_num; i++) {
        th[i].join();
        total_finish += all_clients[i]->req_finish_num;
        total_failed += all_clients[i]->req_failed_num;
        for (int j = 0; j < 5; j++)
            total_search[j] += all_clients[i]->req_search_num[j];
        total_modify += all_clients[i]->req_modify_num;
        total_cas    += all_clients[i]->req_cas_num;
        total_hit    += all_clients[i]->cache_hit_num;
        total_mis    += all_clients[i]->cache_mis_num;
        total_cur    += all_clients[i]->cur_valid_kv_sz;
        total_raw    += all_clients[i]->raw_valid_kv_sz;
        printf("Client %d joined. its tpt is %lu\n", i, (uint64_t)((double)all_clients[i]->req_finish_num / run_time));
    }

    printf("total: %lu ops\n", total_finish);
    printf("failed: %lu ops\n", total_failed);
    for (int j = 0; j < 5; j++)
        printf("search%d: %lu ops\n", j, total_search[j]);
    printf("modify: %lu ops\n", total_modify);
    printf("total tpt: %lu\n", (uint64_t)((double)(total_finish - total_failed) / run_time));   // (ops/s)
    printf("total cas: %lu\n", (uint64_t)((double)total_cas / run_time));   // (ops/s)
    printf("cur_valid_kv_sz: %lu\n", total_cur);
    printf("raw_valid_kv_sz: %lu\n", total_raw);
    printf("[END]\n");

    for (int i = 0; i < thread_num; i++) {
        delete all_clients[i];
    }
    delete addr_cache;
    return 0;
}
