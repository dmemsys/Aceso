#ifndef ACESO_COMMON_H
#define ACESO_COMMON_H

#include <infiniband/verbs.h>
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/coroutine/all.hpp>
#include <boost/crc.hpp>
#include <queue>
#include <immintrin.h>
#include <omp.h>

#define META_AREA_LEN         (1ULL * 1024 * 1024 * 1024)
#define META_AREA_BORDER      (1ULL * 1024 * 1024 * 1024)
#define CLIENT_META_LEN       (1 * 1024 * 1024)

#define MAX_APP_THREAD 65
#define MAX_CORO_NUM 16
#define LATENCY_WINDOWS 10000

#define GB            (1024ll * 1024 * 1024)
#define MB            (1024ll * 1024)
#define KB            (1024ll)
#define MAP_HUGE_2MB  (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB  (30 << MAP_HUGE_SHIFT)
#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))
#define RS_CODE     0x111
#define XOR_CODE    0x222

#ifdef ENABLE_RS_CODE
    #define EC_CODE RS_CODE
#else
    #define EC_CODE XOR_CODE
#endif

#ifdef ENABLE_BOTH_CACHE
    #define CACHE_LEVEL 2
#elif defined(ENABLE_ADDR_CACHE)
    #define CACHE_LEVEL 1
#else
    #define CACHE_LEVEL 0
#endif

#ifdef ENABLE_ASYNC_INDEX
    #define INDEX_REPLICA 1
#else 
    #define INDEX_REPLICA 3
#endif

#ifdef ENABLE_INDEX_SIZE_2048
    #define HASH_AREA_LEN           (2000ULL * 1024 * 1024)     // int max value is 2^31-1 
    #define HASH_AREA_BORDER        (8ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (10)
#elif defined(ENABLE_INDEX_SIZE_1024) 
    #define HASH_AREA_LEN           (1024ULL * 1024 * 1024)
    #define HASH_AREA_BORDER        (5ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (9)
#elif defined(ENABLE_INDEX_SIZE_512) 
    #define HASH_AREA_LEN           (512ULL * 1024 * 1024)
    #define HASH_AREA_BORDER        (3ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (8)
#elif defined(ENABLE_INDEX_SIZE_256) 
    #define HASH_AREA_LEN           (256ULL * 1024 * 1024)
    #define HASH_AREA_BORDER        (3ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (7)
#elif defined(ENABLE_INDEX_SIZE_128) 
    #define HASH_AREA_LEN           (128ULL * 1024 * 1024)
    #define HASH_AREA_BORDER        (2ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (6)
#else
    #define HASH_AREA_LEN           (64ULL * 1024 * 1024)
    #define HASH_AREA_BORDER        (2ULL * 1024 * 1024 * 1024)
    #define RACE_HASH_GLOBAL_DEPTH  (5)
#endif

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

using CheckFunc = std::function<bool ()>;
using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc> >;
struct CoroContext {
    CoroYield *yield;
    CoroCall *master;
    CoroQueue *busy_waiting_queue;
    int coro_id;
};

enum ConnType {
    IB,
    ROCE,
};

namespace define {   // namespace define
    // for exp environment
    constexpr uint32_t memoryNodeNum    = 5;        // [CONFIG]
    constexpr char memoryIPs[16][16] = {            // [CONFIG]
      "10.10.10.1",
      "10.10.10.2",
      "10.10.10.3",
      "10.10.10.4",
      "10.10.10.5",
    };
    constexpr char memcachedIP[16] = "10.10.10.1";  // [CONFIG]
    constexpr char ibDevName[32] = "mlx4_0";        // [CONFIG] r650: mlx5_2;   c6220: mlx4_0
    constexpr uint8_t ibPortId = 1;                 // [CONFIG] r650: 1;        c6220: 1
    constexpr uint8_t ibGidIdx = 0;                 // [CONFIG] r650: 1;        c6220: 0
    constexpr uint8_t ibConnType = IB;              // [CONFIG] r650: ROCE;     c6220: IB
    constexpr uint8_t ibPathMtu = IBV_MTU_1024;     // [CONFIG] r650: 4096;     c6220: 1024

    constexpr uint32_t maxNodeNum       = 32;
    constexpr uint32_t maxClientNum     = 16;
    constexpr uint32_t maxCoroNum       = 8;

    constexpr uint32_t mainCoreID = 0;
    constexpr uint32_t xorCoreID = 1;
    constexpr uint32_t ckptSendCoreID = 2;
    constexpr uint32_t ckptRecvCoreID = 3;

    constexpr uint64_t memoryBlockSize = 2 * MB;    // default value, can be changed
    constexpr uint32_t subblockSize = 1024;
    constexpr int rdmaInlineSize = 256;
    // for ServerMM
    constexpr uint64_t baseAddr = 0x10000000;
    constexpr uint64_t serverMemorySize = 48 * GB;  // 32 * GB;
    
    // for AddrCache
    constexpr float missRateThreash = 0.1;

    // for ClientMM
    constexpr uint64_t coroBufferSize = 4 * MB;
    constexpr uint64_t inputBufferSize = 512 * MB;
    constexpr uint64_t degradeBufferSize = 32 * MB; // for degraded search
    constexpr uint64_t simGCBufferSize = memoryBlockSize;
    constexpr uint64_t clientBufferSize = coroBufferSize * (maxCoroNum + 1) + inputBufferSize + degradeBufferSize + simGCBufferSize;

    // for UD RPC
    constexpr uint32_t messageNR = 1024;
    constexpr uint32_t messagePoolNR = 64 * KB;
    constexpr uint32_t subNR = 64;
    constexpr uint32_t kPoolBatchCount = messagePoolNR / subNR;
    constexpr uint32_t kBatchCount = messageNR / subNR;
    constexpr uint32_t sendPadding = 0;
    constexpr uint32_t recvPadding = 40;
    constexpr uint32_t messageSize = 96;

    // for parity setting
    constexpr uint32_t replicaNum = 3;  // currently only supports 3 replicas
    constexpr uint32_t parityNum = replicaNum - 1;
    constexpr uint32_t primaryNum = memoryNodeNum - parityNum;
    constexpr uint32_t ckptInterval = 500;
    constexpr uint64_t ckptDeltaMaxSize = 96 * MB;
    constexpr uint64_t unXorSeal = 0xFFFFFFFFFFFFFFFF;
    constexpr uint64_t invalidVersion = 0xFFFFFFFFFFFFFFFF;
    constexpr uint32_t versionRound = 256;  // for 8-bit slot version

    // for exp parameters
    constexpr char microLoadSearch[32] = "search";
    constexpr char microLoadInsert[32] = "insert";
    constexpr char microLoadUpdate[32] = "update";
    constexpr char microLoadDelete[32] = "delete";
    constexpr uint32_t microLoadKeyNum = 32 * KB;
    constexpr uint32_t crashServerID = 1;
    constexpr uint32_t codeMethod = EC_CODE;
    constexpr uint32_t codeK = 3;
    constexpr uint32_t codeP = 1;
    constexpr uint32_t xorBlockBatch = 16;
    constexpr uint32_t readBlockBatch = 64;
    constexpr uint32_t cacheLevel = CACHE_LEVEL; // only for breakdown test, 0 means no cache, 1 means normal addr cache, 2 means full power cache
    constexpr uint32_t replicaIndexNum = INDEX_REPLICA; // only for breakdown test
}

struct MMBlockMeta {
    uint8_t block_role;
    uint8_t is_broken;
    union {
        struct {
            uint64_t index_ver;
            uint8_t xor_idx;
        } normal;   
        struct {
            uint64_t delta_addr[define::replicaNum];
            uint8_t xor_map;
        } parity;
    };
};

#endif
