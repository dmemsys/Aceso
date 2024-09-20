#include "KvUtils.h"
#include <immintrin.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <arpa/inet.h>
#include <sys/time.h>
#include <omp.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <string>
#include "isa-l.h"

inline static uint64_t htonll(uint64_t val) {
    return (((uint64_t) htonl(val)) << 32) + htonl(val >> 32);
}
 
inline static uint64_t ntohll(uint64_t val) {
    return (((uint64_t) ntohl(val)) << 32) + ntohl(val >> 32);
}

void serialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        serialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SUBTABLE:
    case REP_ALLOC_SUBTABLE:
        serialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
}

void deserialize_kvmsg(__OUT struct KVMsg * kvmsg) {
    switch (kvmsg->type) {
    case REQ_CONNECT:
    case REP_CONNECT:
        deserialize_conn_info(&kvmsg->body.conn_info);
        break;
    case REQ_ALLOC:
    case REP_ALLOC:
    case REQ_ALLOC_SUBTABLE:
    case REP_ALLOC_SUBTABLE:
        deserialize_mr_info(&kvmsg->body.mr_info);
        break;
    default:
        break;
    }
}

void serialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = htonl(qp_info->qp_num);
    qp_info->lid    = htons(qp_info->lid);
}

void deserialize_qp_info(__OUT struct QpInfo * qp_info) {
    qp_info->qp_num = ntohl(qp_info->qp_num);
    qp_info->lid    = ntohs(qp_info->lid);
}

void serialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = htonll(mr_info->addr);
    mr_info->rkey = htonl(mr_info->rkey);
}

void deserialize_mr_info(__OUT struct MrInfo * mr_info) {
    mr_info->addr = ntohll(mr_info->addr);
    mr_info->rkey = ntohl(mr_info->rkey);
}

void serialize_conn_info(__OUT struct ConnInfo * conn_info) {
    serialize_qp_info(&conn_info->qp_info);
    serialize_mr_info(&conn_info->gc_info);
}

void deserialize_conn_info(__OUT struct ConnInfo * conn_info) {
    deserialize_qp_info(&conn_info->qp_info);
    deserialize_mr_info(&conn_info->gc_info);
}

int load_config(const char * fname, __OUT struct GlobalConfig * config) {
    std::fstream config_fs(fname);

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }

    try {
        config->server_id = pt.get<uint32_t>("server_id");
    } catch (boost::property_tree::ptree_error & e) {
        return -1;
    }
    return 0;
}

void encode_gc_slot(DecodedClientGCSlot * d_gc_slot, __OUT uint64_t * e_gc_slot) {
    uint64_t masked_block_off = (d_gc_slot->pr_addr >> 8) & BLOCK_OFF_BMASK;
    uint64_t masked_pr_addr = (d_gc_slot->pr_addr >> 26) & BLOCK_ADDR_BMASK;
    uint64_t masked_bk_addr = (d_gc_slot->bk_addr >> 26) & BLOCK_ADDR_BMASK;
    uint64_t masked_num_subblock = d_gc_slot->num_subblocks & SUBBLOCK_NUM_BMASK;
    *(e_gc_slot) = (masked_block_off << 46) | (masked_pr_addr << 25) 
        | (masked_bk_addr << 4) | (masked_num_subblock);
}

void decode_gc_slot(uint64_t e_gc_slot, __OUT DecodedClientGCSlot * d_gc_slot) {
    uint64_t block_offset = e_gc_slot >> 46;
    uint64_t pr_block_addr = (e_gc_slot >> 25) & BLOCK_ADDR_BMASK;
    uint64_t bk_block_addr = (e_gc_slot >> 4)  & BLOCK_ADDR_BMASK;
    uint8_t  num_subblocks = e_gc_slot & SUBBLOCK_NUM_BMASK;
    d_gc_slot->pr_addr = (pr_block_addr << 26) | (block_offset << 8);
    d_gc_slot->bk_addr = (bk_block_addr << 26) | (block_offset << 8);
    d_gc_slot->num_subblocks = num_subblocks;
}

int stick_this_thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_CONF);
    if (core_id < 0 || core_id >= num_cores) {
        printf("thread bind core error\n");
        return -1;
    }
    
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    assert(rc == 0);

    cpu_set_t test_cpuset;
    CPU_ZERO(&test_cpuset);
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &test_cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &test_cpuset) && (i != core_id)) {
            printf("thread bind core error, expect core_id %d, get core_id %d\n", core_id, i);
        }
    }
}

uint64_t current_time_us() {
    struct timeval now;
    gettimeofday(&now, NULL);
    return now.tv_usec;
}

void dump_lat_file(char * fname, const std::vector<uint64_t> & lat_vec) {
    if (lat_vec.size() == 0) {
        return;
    }
    FILE * out_fp = fopen(fname, "w");
    for (size_t i = 0; i < lat_vec.size(); i ++) {
        fprintf(out_fp, "%ld\n", lat_vec[i]);
    }
}

std::string get_load_path(std::string &workload_name) {
    assert(workload_name != "");
    if (is_micro_test(workload_name))
        return workload_name;
    return "workloads/" + workload_name + ".spec_load";
    // return "workload-data/" + workload_name + ".spec_load";
}

std::string get_trans_path(std::string &workload_name, uint32_t seq_id) {
    assert(workload_name != "");
    if (is_micro_test(workload_name))
        return workload_name;
    return "workloads/" + workload_name + ".spec_trans" + std::to_string(seq_id);
    // return "workload-data/" + workload_name + ".spec_trans" + std::to_string(seq_id);
}

uint32_t get_unique_id(uint32_t server_id, uint32_t thread_id) {
    uint32_t unique_id;
    if (server_id >= define::memoryNodeNum)
        unique_id = server_id * define::maxClientNum + thread_id; 
    else 
        unique_id = server_id;
    return unique_id;
}

void xor_add_buffers(uint64_t delta_addr, uint64_t parity_addr, size_t length) {
    assert((length % 64) == 0);
    const size_t block_size = 64;

    unsigned char * buffer1 = (unsigned char *)delta_addr;
    unsigned char * buffer2 = (unsigned char *)parity_addr;

    for (size_t i = 0; i < length; i += block_size) {
        __m128i xmm1, xmm2, xmm3, xmm4;

        for (size_t j = 0; j < block_size; j += 16) {
            xmm1 = _mm_load_si128((__m128i*)(buffer1 + i + j));
            xmm2 = _mm_load_si128((__m128i*)(buffer2 + i + j));

            xmm3 = _mm_xor_si128(xmm1, xmm2);

            _mm_store_si128((__m128i*)(buffer2 + i + j), xmm3);
        }
    }
}

#define force_inline __attribute__((always_inline)) inline
static force_inline void
compiler_barrier(void)
{
	asm volatile("" ::: "memory");
}

static force_inline void
mm_stream_si128(__m128i *dest, __m128i src)
{
	_mm_stream_si128(dest, src);
	compiler_barrier();
}

void avx_memcpy_128_ckpt(uint64_t index_addr, uint64_t ckpt_addr, uint64_t xor_dest, size_t size) {
    size_t i;
    size_t vec_size = sizeof(__m128i);
    size_t vec_count = size / vec_size;
    size_t remainder = size % vec_size;
    assert(remainder == 0);

    __m128i* d = (__m128i*)ckpt_addr;
    const __m128i* s = (const __m128i*)index_addr;

    __m128i* x_d = (__m128i*)xor_dest;
    const __m128i* x_s = (const __m128i*)ckpt_addr;

    for (i = 0; i < vec_count; i++) {
        __m128i xmm1, xmm2, xmm3;

        xmm1 = _mm_loadu_si128(s + i);
        xmm2 = _mm_loadu_si128(x_s + i);
        
        _mm_storeu_si128(d + i, xmm1);
        xmm3 = _mm_xor_si128(xmm1, xmm2);
        _mm_storeu_si128(x_d + i, xmm3);
    }
}

void avx_memcpy_128(uint64_t dest, uint64_t src, uint64_t xor_dest, uint64_t xor_src, size_t size) {
    size_t i;
    size_t vec_size = sizeof(__m128i);
    size_t vec_count = size / vec_size;
    size_t remainder = size % vec_size;
    assert(remainder == 0);

    __m128i* d = (__m128i*)dest;
    const __m128i* s = (const __m128i*)src;

    __m128i* x_d = (__m128i*)xor_dest;
    const __m128i* x_s = (const __m128i*)xor_src;

    for (i = 0; i < vec_count; i++) {
        __m128i xmm1, xmm2, xmm3;

        xmm1 = _mm_loadu_si128(s + i);
        xmm2 = _mm_loadu_si128(x_s + i);
        
        _mm_storeu_si128(d + i, xmm1);
        xmm3 = _mm_xor_si128(xmm1, xmm2);
        _mm_storeu_si128(x_d + i, xmm3);
    }
}

void avx_memcpy_256(uint64_t dest, uint64_t src, uint64_t xor_dest, uint64_t xor_src, size_t size) {

    size_t vec_size = sizeof(__m256i);
    size_t vec_count = size / vec_size;
    size_t remainder = size % vec_size;
    assert(remainder == 0);

    __m256i* d = (__m256i*)dest;
    const __m256i* s = (const __m256i*)src;

    __m256i* x_d = (__m256i*)xor_dest;
    const __m256i* x_s = (const __m256i*)xor_src;

    for (size_t i = 0; i < vec_count; i++) {
        __m256i ymm1, ymm2, ymm3;

        ymm1 = _mm256_load_si256(s + i);
        ymm2 = _mm256_load_si256(x_s + i);
        ymm3 = (__m256i)_mm256_xor_pd((__m256d)ymm1, (__m256d)ymm2);
        _mm256_store_si256(d + i, ymm1);
        _mm256_store_si256(x_d + i, ymm3);
    }
}

void avx_xor_buffers(uint64_t addr1, uint64_t addr2, uint64_t addr3, size_t length) {
    const size_t block_size = 16;

    unsigned char* buffer1 = (unsigned char*)addr1;
    unsigned char* buffer2 = (unsigned char*)addr2;
    unsigned char* buffer3 = (unsigned char*)addr3;

    for (size_t i = 0; i < length; i += block_size) {
        __m128i xmm1, xmm2, xmm3;

        xmm1 = _mm_loadu_si128((__m128i*)(buffer1 + i));
        xmm2 = _mm_loadu_si128((__m128i*)(buffer2 + i));
        xmm3 = _mm_xor_si128(xmm1, xmm2);
        _mm_storeu_si128((__m128i*)(buffer3 + i), xmm3);
    }
}

bool is_micro_test(std::string & workload_name) {
    if (workload_name == define::microLoadSearch || 
        workload_name == define::microLoadInsert ||
        workload_name == define::microLoadUpdate ||
        workload_name == define::microLoadDelete ||
        workload_name == "update-consume")
        return true;

    return false;
}

double get_run_time(std::string & workload_name) {
    if (workload_name == define::microLoadSearch)
        return 10;
    if (workload_name == define::microLoadInsert)
        return 2;
    if (workload_name == define::microLoadUpdate)
        return 10;
    if (workload_name == define::microLoadDelete)
        return 0.5;
    if (workload_name == "workloadd")
        return 5;
    if (workload_name == "update-consume")
        return 35;
    if (workload_name.find("upd") != std::string::npos)
        return 20;

    return 10;
}

std::string micro_get_op_type(std::string & workload_name) {
    if (workload_name == define::microLoadSearch) {
        return "READ";
    }
    if (workload_name == define::microLoadInsert) {
        return "INSERT";
    }
    if (workload_name == define::microLoadUpdate || workload_name == "update-consume") {
        return "UPDATE";
    }
    if (workload_name == define::microLoadDelete) {
        return "DELETE";
    }
}

int gf_gen_decode_matrix(unsigned char *encode_matrix,
				unsigned char *decode_matrix,
				unsigned char *invert_matrix,
				unsigned int *decode_index,
				unsigned char *src_err_list,
				unsigned char *src_in_err,
				int nerrs, int nsrcerrs, int k, int m)
{
	int i, j, p;
	int r;
	unsigned char *backup, *b, s;
	int incr = 0;

	b = (unsigned char *)malloc(MMAX * KMAX);
	backup = (unsigned char *)malloc(MMAX * KMAX);

	if (b == NULL || backup == NULL) {
		printf("Test failure! Error with malloc\n");
		free(b);
		free(backup);
		return -1;
	}
	// Construct matrix b by removing error rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r])
			r++;
		for (j = 0; j < k; j++) {
			b[k * i + j] = encode_matrix[k * r + j];
			backup[k * i + j] = encode_matrix[k * r + j];
		}
		decode_index[i] = r;
	}
	incr = 0;
	while (gf_invert_matrix(b, invert_matrix, k) < 0) {
		if (nerrs == (m - k)) {
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		incr++;
		memcpy(b, backup, MMAX * KMAX);
		for (i = nsrcerrs; i < nerrs - nsrcerrs; i++) {
			if (src_err_list[i] == (decode_index[k - 1] + incr)) {
				// skip the erased parity line
				incr++;
				continue;
			}
		}
		if (decode_index[k - 1] + incr >= m) {
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		decode_index[k - 1] += incr;
		for (j = 0; j < k; j++)
			b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];

	};

	for (i = 0; i < nsrcerrs; i++) {
		for (j = 0; j < k; j++) {
			decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
		}
	}
	/* src_err_list from encode_matrix * invert of b for parity decoding */
	for (p = nsrcerrs; p < nerrs; p++) {
		for (i = 0; i < k; i++) {
			s = 0;
			for (j = 0; j < k; j++)
				s ^= gf_mul(invert_matrix[j * k + i],
					    encode_matrix[k * src_err_list[p] + j]);

			decode_matrix[k * p + i] = s;
		}
	}
	free(b);
	free(backup);
	return 0;
}