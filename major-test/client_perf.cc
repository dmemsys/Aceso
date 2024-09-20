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
int block_size;
std::string workload_name;  // parsed args
AddrCache * addr_cache;
Client * all_clients[define::maxClientNum];
std::thread th[MAX_APP_THREAD];
std::atomic<int32_t> warmup_cnt{0};

int merge_lat(std::string type, std::string op, int thread_num) {
    std::string directoryPath = "results/";
    std::string outputFileName = type + "_" + op + "_lat" + ".txt";
    std::ofstream output(directoryPath + outputFileName);

    if (!output.is_open()) {
        std::cout << "cannot open output file" << std::endl;
        return 1;
    }

    int req_type;
    if (op == "insert") {
        req_type = KV_OP_INSERT;
    } else if (op == "delete") {
        req_type = KV_OP_DELETE;
    } else if (op == "update") {
        req_type = KV_OP_UPDATE;
    } else if (op == "search") {
        req_type = KV_OP_SEARCH;
    } else {
        printf("wrong op_type\n");
        return -1;
    }
    
    std::map<int, int> latencyCountMap;
    for (int i = 0; i < thread_num; i++) {
        for (int j = 0; j < LATENCY_WINDOWS; j++) {
            int latency = j;
            int count = all_clients[i]->req_latency[req_type-1][j];
            if (count > 0) {
                latencyCountMap[latency] += count;
            } 
        }
    }
    
    for (auto it = latencyCountMap.begin(); it != latencyCountMap.end(); ++it) {
        output << it->first << " " << it->second << std::endl;
    }
    output.close();
    return 0;
}

int merge_lat_all() {
    std::map<std::string, std::vector<std::string>> workload_dict;
    workload_dict["search"] = { "search" };
    workload_dict["update"] = { "update" };
    workload_dict["insert"] = { "insert" };
    workload_dict["delete"] = { "delete" };
    workload_dict["workloada"] = { "search", "update" };
    workload_dict["workloadb"] = { "search", "update" };
    workload_dict["workloadc"] = { "search" };
    workload_dict["workloadd"] = { "search", "insert" };
    workload_dict["workloadtwic"] = { "search", "update" };
    workload_dict["workloadtwis"] = { "search" };
    workload_dict["workloadtwit"] = { "search", "update" };

    if (workload_dict.count(workload_name) > 0 && coro_num == 1) {
        std::vector<std::string> op_list = workload_dict[workload_name];
        if (is_micro_test(workload_name)) {
            for(auto & op : op_list) {
                merge_lat("micro", op, thread_num);
            }
        } else {
            for(auto & op : op_list) {
                merge_lat("ycsb", op, thread_num);
            }
        }
    }
}

void thread_load(Client *client, std::string workload_name, std::string op_type = "") {
    int32_t num_ops = -1;
    if (is_micro_test(workload_name)) {
        op_type = std::string("INSERT");
        num_ops = define::microLoadKeyNum;
    }
    client->load_kv_requests(0, num_ops, get_load_path(workload_name), op_type);
    client->init_kv_req_space(0, 0, client->req_local_num, nullptr);

    printf("Load phase start.\n");
    
    int ret = 0;
    bool should_stop = false;
    uint32_t num_failed = 0;
    void * search_addr = NULL;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (uint32_t i = 0; i < client->req_local_num; i++) {
        KVReqCtx * ctx = &client->kv_req_ctx_list[i];
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
                do {
                    ret = client->kv_insert(ctx);
                } while (ret == KV_OPS_FAIL_REDO);
                if (ret == KV_OPS_FAIL_RETURN) {
                    num_failed++;
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

    printf("Load phase ends (failed %d).\n", num_failed);
    printf("Time spent: %ld ms.\n", duration.count());
    return;
}

ReturnValue work_func(Client * client, KVReqCtx * ctx) {
    ReturnValue ret;
    switch (ctx->req_type) {
        case KV_OP_SEARCH:
            ret.value_addr = client->kv_search(ctx);
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

    Client * client = new Client(my_server_id, my_thread_id, (cn_num * thread_num), addr_cache, false, block_size);
    all_clients[my_thread_id] = client;

    printf("Client %d start.\n", my_thread_id);

    // insert load
    if (is_micro_test(workload_name)) {
        if (micro_get_op_type(workload_name) != "INSERT")
            thread_load(client, workload_name);
        client->client_barrier("load-load-complete");
    } else {
        if (my_server_id == define::memoryNodeNum && my_thread_id == 0) {
            thread_load(client, workload_name);
            client->client_barrier("load-load-complete");
        } else {
            client->client_barrier("load-load-complete");
        }
    } 
    printf("Client %d passed load barrier.\n", my_thread_id);

    // load ycsb_trans
    uint32_t seq_id = (my_server_id - define::memoryNodeNum) * thread_num + my_thread_id;

    if (is_micro_test(workload_name))
        client->load_kv_requests(0, define::microLoadKeyNum, get_trans_path(workload_name, seq_id), micro_get_op_type(workload_name));
    else
        client->load_kv_requests(0, -1, get_trans_path(workload_name, seq_id));
    printf("Client %d load trans %s.\n", my_thread_id, get_trans_path(workload_name, seq_id).c_str());

    client->client_barrier("load-trans-complete");

    // start ycsb test
    warmup_cnt.fetch_add(1);
    client->run_coroutine(should_stop, work_func, coro_num);

    printf("Client %d exit.\n", my_thread_id);
}

void parse_args(int argc, char *argv[]) {
    if (argc != 5 && argc != 6) {
        printf("Usage: ./client_perf workload_name cn_num thread_num coro_num <block_size>\n");
        exit(-1);
    }
    workload_name = std::string(argv[1]);
    cn_num = atoi(argv[2]);
    thread_num = atoi(argv[3]); assert(thread_num <= define::maxClientNum);
    coro_num = atoi(argv[4]);
    block_size = define::memoryBlockSize;
    if (argc == 6) {
        block_size = atoi(argv[5]);
    }
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

    double run_time = get_run_time(workload_name);
    
    clock_gettime(CLOCK_REALTIME, &exp_st);
    usleep(run_time * 1000000);
    should_stop = true;
    clock_gettime(CLOCK_REALTIME, &exp_ed);

    merge_lat_all();

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

    for (int i = 0; i < thread_num; i++)
        delete all_clients[i];
    delete addr_cache;

    return 0;
}
