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

ReturnValue work_func(Client * client, KVReqCtx * ctx) {
    ReturnValue ret;
    switch (ctx->req_type) {
        case KV_OP_SEARCH:
            ret.value_addr = client->kv_search_degrade(ctx);
            break;
        case KV_OP_INSERT:
            ret.ret_code = client->kv_insert(ctx);
            break;
        case KV_OP_UPDATE:
            ret.ret_code = client->kv_update(ctx);
            break;
        case KV_OP_DELETE:
            ret.ret_code = client->kv_delete(ctx);
            break;
        default:
            ret.value_addr = client->kv_search(ctx);
            break;
    }
    return ret;
}

void thread_run(int my_thread_id, volatile bool *should_stop) {
    stick_this_thread_to_core(my_thread_id + 2);

    Client * client = new Client(my_server_id, my_thread_id, (cn_num * thread_num), addr_cache, true);
    client->start_degrade(define::crashServerID);
    all_clients[my_thread_id] = client;

    printf("Client %d start.\n", my_thread_id);

    // load ycsb_trans
    uint32_t seq_id = (my_server_id - define::memoryNodeNum) * thread_num + my_thread_id;

    if (is_micro_test(workload_name))
        client->load_kv_requests(0, define::microLoadKeyNum, get_trans_path(workload_name, seq_id), "READ");
    else
        client->load_kv_requests(0, -1, get_trans_path(workload_name, seq_id));
    printf("Client %d load trans %s.\n", my_thread_id, get_trans_path(workload_name, seq_id).c_str());

    client->client_barrier("after-crash-load-trans-complete");

    // start ycsb test
    warmup_cnt.fetch_add(1);
    client->run_coroutine(should_stop, work_func, coro_num);

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

// block_size = 2MB by default
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
    uint64_t total_finish = 0;
    uint64_t total_failed = 0;

    while (warmup_cnt.load() != thread_num)
        ;

    clock_gettime(CLOCK_REALTIME, &exp_st);
    sleep(5);
    should_stop = true;
    clock_gettime(CLOCK_REALTIME, &exp_ed);

    for (int i = 0; i < thread_num; i++) {
        th[i].join();
        total_finish += all_clients[i]->req_finish_num;
        total_failed += all_clients[i]->req_failed_num;
        printf("Client %d joined.\n", i);
    }

    printf("total: %lu ops\n", total_finish);
    printf("failed: %lu ops\n", total_failed);
    printf("total tpt: %lu\n", (total_finish - total_failed) / 5);   // (ops/s)
    printf("[END]\n");

    for (int i = 0; i < thread_num; i++) {
        delete all_clients[i];
    }
    delete addr_cache;
    return 0;
}
