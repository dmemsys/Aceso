#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "Server.h"

int main(int argc, char ** argv) {
    if (argc != 1 && argc != 2) {
        printf("Usage: %s <block_size>\n", argv[0]);
        return -1;
    }
    int ckpt_interval = define::ckptInterval;
    int block_size = define::memoryBlockSize;
    if (argc >= 2)
        block_size = atoi(argv[1]);

    GlobalConfig config;
    int ret = load_config("./config.json", &config); assert(ret == 0);
    int32_t server_id = config.server_id;

    printf("===== Starting Server %d =====\n", server_id);
    Server * server = new Server((uint8_t)server_id, true, ckpt_interval, block_size);
    ServerMainArgs server_main_args;
    server_main_args.server = server;

    pthread_t server_tid;
    pthread_create(&server_tid, NULL, server_main, (void *)&server_main_args);

    printf("===== Ending Server %d =====\n", server_id);
    printf("[END]\n");

    sleep(100000000ll);

    server->stop();
    return 0;
}