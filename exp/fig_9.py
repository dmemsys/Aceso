# micro-test -- latency
from func_timeout import FunctionTimedOut
from pathlib import Path
import json

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.color_printer import print_GOOD, print_WARNING, print_OK
from utils.func_timer import print_func_time


input_path = './params'
style_path = "./styles"
output_path = './results'
fig_num = '9'
small_fig_num = {'YCSB A': 'a', 'YCSB B': 'b', 'YCSB C': 'c', 'YCSB D': 'd'}

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
ycsb_dir      = f'{home_dir}/aceso/ycsb-test'
client_ips    = params['client_ips']
master_ip     = params['master_ip']
server_ips    = params['server_ips']
cmake_options = params['cmake_options']

# fig params
with (Path(input_path) / f'fig_{fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
methods            = fig_params['methods']
workload_names     = fig_params['workload_names']
target_epochs      = fig_params['target_epoch']
CN_and_client_nums = fig_params['client_num']  
MN_num             = fig_params['MN_num']
workload_ops       = fig_params['workload_ops']

@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    op_names = ['insert', 'update', 'search', 'delete']
    method_ops = []
    for method in methods:
        for op in op_names:
            method_op_p50 = f"{method}_{op}_p50"
            method_op_p99 = f"{method}_{op}_p99"
            method_ops.append(method_op_p50)
            method_ops.append(method_op_p99)
    plot_data = {
        'methods': methods,
        'bar_groups': op_names,
        'Y_data': {method_op: [] for method_op in method_ops}
    }
    for method in methods:
        if method == 'fusee':
            project_dir = f"{home_dir}/{method}"
            server_work_dir = f"{project_dir}/build/ycsb-test"
            client_work_dir = f"{project_dir}/build/micro-test"
            server_env_cmd = f"cd {server_work_dir}"
            client_env_cmd = f"cd {client_work_dir}"

            # only for aceso
            memcache_dir = f"{project_dir}/src"
            memcache_env_cmd = f"cd {memcache_dir}"

            # update and build project
            cmake_option = cmake_options[method]    # TODO
            SERVER_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd ycsb-test && mkdir -p results"
            CLIENT_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd micro-test && mkdir -p results"
            SET_CN_HUGEPAGE = "echo 2000 | sudo tee /proc/sys/vm/nr_hugepages"
            SET_MN_HUGEPAGE = "echo 20000 | sudo tee /proc/sys/vm/nr_hugepages"
            UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
            KILL_SERVER = f"{server_env_cmd} && mkdir -p results && killall -9 ycsb_test_server"
            KILL_CLIENT = f"{client_env_cmd} && mkdir -p results && killall -9 micro_test_lat_multi_client"
            cmd.all_execute(KILL_CLIENT)
            cmd.all_execute(UNSET_HUGEPAGE)
            cmd.all_execute(CLIENT_BUILD_PROJECT, -1, True)
            cmd.server_all_execute(KILL_SERVER)
            cmd.server_all_execute(UNSET_HUGEPAGE)
            cmd.server_all_execute(SERVER_BUILD_PROJECT, 0, -1, True)

            CN_num = len(client_ips)
            client_num_per_CN = 8
            
            if method == 'aceso':
                SERVER_START = f"{memcache_env_cmd} && ./run_memcached.sh && {server_env_cmd} && ./ycsb_test_server"
            else:
                SERVER_START = f"{server_env_cmd} && ./ycsb_test_server"
            MICRO_TEST = f"{client_env_cmd} && ./micro_test_lat_multi_client ./client_config.json {client_num_per_CN} {CN_num}"
            
            while True:
                try:
                    cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
                    cmd.server_all_long_execute(SERVER_START, 0, MN_num)
                    cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
                    logs = cmd.all_long_execute(MICRO_TEST, CN_num)
                    
                    cmd.all_execute(KILL_CLIENT, CN_num)
                    cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                    cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                    cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                    
                    op_p50_p99_lats = cmd.get_cluster_lats('micro', str(Path(client_work_dir) / 'results'), op_names, CN_num, client_num_per_CN, 1)
                    # tpt, _, _, _ = tp.get_statistics(logs, target_epochs[workload])
                    # total_tpt = tp.get_total_tpt(logs)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")
                    cmd.all_execute(KILL_CLIENT, CN_num)
                    cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                    cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                    cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)

            print_GOOD(f"[FINISHED POINT] method={method} client_num={CN_num*client_num_per_CN}")
            for op in op_names:
                p50_lat, p99_lat = op_p50_p99_lats[op]
                # plot_data['Y_data'][method][op] = p99_lat
                method_op_p50 = f"{method}_{op}_p50"
                method_op_p99 = f"{method}_{op}_p99"
                plot_data['Y_data'][method_op_p50].append(p50_lat)
                plot_data['Y_data'][method_op_p99].append(p99_lat)
        elif method == 'aceso':
                project_dir = f"{home_dir}/{method}"
                build_dir = f"{project_dir}/build"
                work_dir = f"{project_dir}/build/major-test"
                env_cmd = f"cd {work_dir}"

                # only for aceso
                memcache_dir = f"{project_dir}/src"
                memcache_env_cmd = f"cd {memcache_dir}"

                # update and build project
                cmake_option = cmake_options[method]    # TODO
                BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd major-test && mkdir -p results"
                SET_CN_HUGEPAGE = "echo 4000 | sudo tee /proc/sys/vm/nr_hugepages"
                SET_MN_HUGEPAGE = "echo 28000 | sudo tee /proc/sys/vm/nr_hugepages"
                UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
                KILL_SERVER = f"{env_cmd} && mkdir -p results && killall -9 server"
                KILL_CLIENT = f"{env_cmd} && mkdir -p results && killall -9 client_perf"
                cmd.all_execute(KILL_CLIENT)
                cmd.all_execute(UNSET_HUGEPAGE)
                cmd.all_execute(BUILD_PROJECT, -1, True)
                cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                cmd.server_all_execute(BUILD_PROJECT, 0, MN_num, True)

                CN_num = len(client_ips)
                client_num_per_CN = 8
                
                for op in op_names:
                    SERVER_START = f"{memcache_env_cmd} && ./run_memcached.sh && {env_cmd} && ./server"
                    YCSB_TEST = f"{env_cmd} && ./client_perf {op} {CN_num} {client_num_per_CN} 1"

                    while True:
                        try:
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
                            cmd.server_all_long_execute(SERVER_START, 0, MN_num)
                            cmd.all_execute(KILL_CLIENT)
                            cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
                            logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                            op_p50_p99_lats = cmd.get_cluster_lats('micro', str(Path(work_dir) / 'results'), [op], CN_num, client_num_per_CN, 1)
                            
                            cmd.all_execute(KILL_CLIENT, CN_num)
                            cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)

                            break
                        except (FunctionTimedOut, Exception) as e:
                            print_WARNING(f"Error! Retry... {e}")

                    print_GOOD(f"[FINISHED POINT] workload={op} method={method} client_num={CN_num*client_num_per_CN}")
                
                    p50_lat, p99_lat = op_p50_p99_lats[op]
                    method_op_p50 = f"{method}_{op}_p50"
                    method_op_p99 = f"{method}_{op}_p99"

                    plot_data['Y_data'][method_op_p50].append(p50_lat)
                    plot_data['Y_data'][method_op_p99].append(p99_lat)
        else:
            # for clover only
            clover_op_names = ['insert', 'update', 'search']
            clover_MN_num = MN_num - 1      # 1 MN became MS
            project_dir = f"{home_dir}/{method}/clover"
            env_cmd = f"cd {project_dir}"

            KILL_PREV_EXP = f"{env_cmd} && ./local_kill.sh"
            MKDIR_RESULTS = f"{env_cmd} && rm -rf results/ && mkdir -p results"
            ENABLE_HUGEPAGE = f"{env_cmd} && ./hugepages-create.sh"
            cmd.all_execute(MKDIR_RESULTS)  # only clients
            
            cmd.all_execute(ENABLE_HUGEPAGE)
            cmd.clover_meta_execute(ENABLE_HUGEPAGE)
            cmd.clover_memo_all_execute(ENABLE_HUGEPAGE)
            
            cmd.all_execute(KILL_PREV_EXP)
            cmd.clover_meta_execute(KILL_PREV_EXP)
            cmd.clover_memo_all_execute(KILL_PREV_EXP)
            
            CN_num = len(client_ips)
            client_num_per_CN = 8
            while True:
                try:
                    cmd.all_execute(KILL_PREV_EXP, CN_num)
                    cmd.clover_meta_execute(KILL_PREV_EXP)
                    cmd.clover_memo_all_execute(KILL_PREV_EXP, clover_MN_num)
                    
                    cmd.clover_meta_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 0, 1, 50, "upd0")
                    cmd.clover_memo_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 0, 1, 50, "upd0")
                    cmd.clover_clie_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 0, 1, 50, "upd0")
                    
                    cmd.all_execute(KILL_PREV_EXP, CN_num)
                    cmd.clover_meta_execute(KILL_PREV_EXP)
                    cmd.clover_memo_all_execute(KILL_PREV_EXP, clover_MN_num)
                    op_p50_p99_lats = cmd.get_cluster_lats('micro', str(Path(project_dir) / 'results'), clover_op_names, CN_num, client_num_per_CN, 1)
                    break
                except (FunctionTimedOut, Exception) as e:
                    print_WARNING(f"Error! Retry... {e}")

            print_GOOD(f"[FINISHED POINT] method={method} client_num={CN_num*client_num_per_CN}")
            for op in clover_op_names:
                p50_lat, p99_lat = op_p50_p99_lats[op]
                plot_data['Y_data'][method][op] = p99_lat
            
            plot_data['Y_data'][method]['delete'] = 0   # clover doesn't have delete operation
                
    # save data
    Path(output_path).mkdir(exist_ok=True)
    with (Path(output_path) / f'fig_{fig_num}a.json').open(mode='w') as f:
        json.dump(plot_data, f, indent=2)

if __name__ == '__main__':
    print_WARNING("remember to sudo su && remember first CN's server_id = 0")
    cmd = CMDManager(client_ips, master_ip, server_ips)
    tp = LogParser()
    t = main(cmd, tp)

