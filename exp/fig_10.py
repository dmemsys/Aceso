# ycsb throughput test
from func_timeout import FunctionTimedOut
from pathlib import Path
import json
import threading

from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.color_printer import print_GOOD, print_WARNING, print_OK
from utils.func_timer import print_func_time

input_path = './params'
style_path = "./styles"
output_path = './results'
fig_num = '10'
small_fig_num = {'YCSB A': 'a', 'YCSB B': 'b', 'YCSB C': 'c', 'YCSB D': 'd'}

# common params
with (Path(input_path) / f'common.json').open(mode='r') as f:
    params = json.load(f)
home_dir      = params['home_dir']
client_ips    = params['client_ips']
master_ip     = params['master_ip']
server_ips    = params['server_ips']

# fig params
with (Path(input_path) / f'fig_{fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
methods            = fig_params['methods']
workload_names     = fig_params['workload_names']
target_epochs      = fig_params['target_epoch']
CN_and_client_nums = fig_params['client_num']  
MN_num             = fig_params['MN_num']

@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    for workload, workload_name in workload_names.items():
        plot_data = {
            'methods': methods,
            'X_data': {method: [] for method in methods},
            'Y_data': {method: [] for method in methods}
        }
        for method in methods:
            if method == 'fusee':
                project_dir = f"{home_dir}/{method}"
                build_dir = f"{project_dir}/build"
                work_dir = f"{project_dir}/build/ycsb-test"
                env_cmd = f"cd {work_dir}"

                # only for aceso
                memcache_dir = f"{project_dir}/src"
                memcache_env_cmd = f"cd {memcache_dir}"

                # update and build project
                BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && cmake .. && make -j"
                SET_CN_HUGEPAGE = "echo 2000 | sudo tee /proc/sys/vm/nr_hugepages"
                SET_MN_HUGEPAGE = "echo 20000 | sudo tee /proc/sys/vm/nr_hugepages"
                UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
                KILL_SERVER = f"{env_cmd} && killall -9 ycsb_test_server"
                KILL_CLIENT = f"{env_cmd} && killall -9 ycsb_test_multi_client"
                cmd.all_execute(KILL_CLIENT)
                cmd.all_execute(UNSET_HUGEPAGE)
                cmd.all_execute(BUILD_PROJECT, -1, True)
                cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                cmd.server_all_execute(BUILD_PROJECT, 0, MN_num, True)

                for CN_num, client_num_per_CN in CN_and_client_nums[method][workload]:
                    # CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
                    # SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split-workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
                    if method == 'aceso':
                        SERVER_START = f"{memcache_env_cmd} && ./run_memcached.sh && {env_cmd} && ./ycsb_test_server"
                    else:
                        SERVER_START = f"{env_cmd} && ./ycsb_test_server"
                    YCSB_TEST = f"{env_cmd} && ./ycsb_test_multi_client ./client_config.json {workload_name} {client_num_per_CN}"
                    KILL_PROCESS = f"{env_cmd} && killall -9 ycsb_test_multi_client"

                    while True:
                        try:
                            cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
                            cmd.server_all_long_execute(SERVER_START, 0, MN_num)
                            cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
                            logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                            total_tpt = tp.get_total_tpt(logs)
                            
                            cmd.all_execute(KILL_CLIENT, CN_num)
                            cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                        
                            break
                        except (FunctionTimedOut, Exception) as e:
                            print_WARNING(f"Error! Retry... {e}")
                            cmd.all_execute(KILL_CLIENT, CN_num)
                            cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)

                    print_GOOD(f"[FINISHED POINT] workload={workload} method={method} client_num={CN_num*client_num_per_CN} tpt={total_tpt}")
                    plot_data['X_data'][method].append(CN_num*client_num_per_CN)
                    plot_data['Y_data'][method].append(total_tpt)
            elif method == 'aceso':
                project_dir = f"{home_dir}/{method}"
                work_dir = f"{project_dir}/build/major-test"
                env_cmd = f"cd {work_dir}"

                # only for aceso
                memcache_dir = f"{project_dir}/src"
                memcache_env_cmd = f"cd {memcache_dir}"

                # update and build project
                BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && cmake .. && make -j"
                SET_CN_HUGEPAGE = "echo 4000 | sudo tee /proc/sys/vm/nr_hugepages"
                SET_MN_HUGEPAGE = "echo 28000 | sudo tee /proc/sys/vm/nr_hugepages"
                UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
                KILL_SERVER = f"{env_cmd} && killall -9 server"
                KILL_CLIENT = f"{env_cmd} && killall -9 client_perf"
                cmd.all_execute(KILL_CLIENT)
                cmd.all_execute(UNSET_HUGEPAGE)
                cmd.all_execute(BUILD_PROJECT, -1, True)
                cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                cmd.server_all_execute(BUILD_PROJECT, 0, MN_num, True)

                for CN_num, client_num_per_CN in CN_and_client_nums[method][workload]:
                    # CLEAR_MEMC = f"{env_cmd} && /bin/bash ../script/restartMemc.sh"
                    # SPLIT_WORKLOADS = f"{env_cmd} && python3 {ycsb_dir}/split-workload.py {workload_name} {key_type} {CN_num} {client_num_per_CN}"
                    SERVER_START = f"{memcache_env_cmd} && ./run_memcached.sh && {env_cmd} && ./server"
                    YCSB_TEST = f"{env_cmd} && ./client_perf {workload_name} {CN_num} {client_num_per_CN} 8"

                    while True:
                        try:
                            cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
                            cmd.server_all_long_execute(SERVER_START, 0, MN_num)
                            cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
                            logs = cmd.all_long_execute(YCSB_TEST, CN_num)
                            total_tpt = tp.get_total_tpt(logs)
                            
                            cmd.all_execute(KILL_CLIENT, CN_num)
                            cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                        
                            break
                        except (FunctionTimedOut, Exception) as e:
                            print_WARNING(f"Error! Retry... {e}")
                            cmd.all_execute(KILL_CLIENT, CN_num)
                            cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                            cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                            cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)

                    print_GOOD(f"[FINISHED POINT] workload={workload} method={method} client_num={CN_num*client_num_per_CN} tpt={total_tpt}")
                    plot_data['X_data'][method].append(CN_num*client_num_per_CN)
                    plot_data['Y_data'][method].append(total_tpt)
            elif method == 'clover':
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
                
                clover_workload_to_val = {
                    'YCSB A' : 50,
                    'YCSB B' : 5,
                    'YCSB C' : 0,
                    'YCSB D' : 500
                }
                ycsb_num = clover_workload_to_val[workload]
                for CN_num, client_num_per_CN in CN_and_client_nums[method][workload]:
                    while True:
                        try:
                            cmd.all_execute(KILL_PREV_EXP, CN_num)
                            cmd.clover_meta_execute(KILL_PREV_EXP)
                            cmd.clover_memo_all_execute(KILL_PREV_EXP, clover_MN_num)
                            
                            cmd.clover_meta_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 1, 0, ycsb_num, "upd0")
                            cmd.clover_memo_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 1, 0, ycsb_num, "upd0")
                            logs = cmd.clover_clie_start_exp(env_cmd, CN_num, clover_MN_num, client_num_per_CN, 1, 0, ycsb_num, "upd0")
                            total_tpt = tp.get_total_tpt(logs)
                            
                            cmd.all_execute(KILL_PREV_EXP, CN_num)
                            cmd.clover_meta_execute(KILL_PREV_EXP)
                            cmd.clover_memo_all_execute(KILL_PREV_EXP, clover_MN_num)
                            break
                        except (FunctionTimedOut, Exception) as e:
                            print_WARNING(f"Error! Retry... {e}")

                    print_GOOD(f"[FINISHED POINT] workload={workload} method={method} client_num={CN_num*client_num_per_CN} tpt={total_tpt}")
                    plot_data['X_data'][method].append(CN_num*client_num_per_CN)
                    plot_data['Y_data'][method].append(total_tpt)
            else:
                pass
                
            # save data after each round 
            Path(output_path).mkdir(exist_ok=True)
            with (Path(output_path) / f'fig_{fig_num}{small_fig_num[workload]}.json').open(mode='w') as f:
                json.dump(plot_data, f, indent=2)


if __name__ == '__main__':
    print_WARNING("remember to sudo su && remember first CN's server_id = 0")
    cmd = CMDManager(client_ips, master_ip, server_ips)
    tp = LogParser()
    t = main(cmd, tp)
    with (Path(output_path) / 'time.log').open(mode="a+") as f:
        f.write(f"fig_{fig_num}.py execution time: {int(t//60)} min {int(t%60)} s\n")