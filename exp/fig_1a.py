# PS: set micro_test/client_config_rep2.json & rep1.json
# pre-test throughput & CAS number
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
fig_num = '1a'
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
MN_num             = fig_params['MN_num']
index_replicas     = fig_params['index_replicas']


@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    op_names = ['insert', 'update', 'search', 'delete']
    plot_data = {
        'methods': methods,
        'bar_groups': op_names,
        'Y_data': {
            replica_num: {}
            for replica_num in index_replicas
        },
        'CAS_data': {
            replica_num: {}
            for replica_num in index_replicas
        }
    }

    for method in methods:
        if method == 'fusee':
            project_dir = f"{home_dir}/{method}"
            server_work_dir = f"{project_dir}/build/ycsb-test"
            client_work_dir = f"{project_dir}/build/micro-test"
            server_env_cmd = f"cd {server_work_dir}"
            client_env_cmd = f"cd {client_work_dir}"

            # update and build project
            cmake_option = cmake_options[method]    # TODO
            SERVER_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd ycsb-test && mkdir -p results"
            CLIENT_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd micro-test && mkdir -p results"
            SET_CN_HUGEPAGE = "echo 2000 | sudo tee /proc/sys/vm/nr_hugepages"
            SET_MN_HUGEPAGE = "echo 20000 | sudo tee /proc/sys/vm/nr_hugepages"
            UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
            KILL_SERVER = f"{server_env_cmd} && killall -9 ycsb_test_server"
            KILL_CLIENT = f"{client_env_cmd} && killall -9 micro_test_tpt_multi_client"
            cmd.all_execute(KILL_CLIENT)
            cmd.all_execute(UNSET_HUGEPAGE)
            cmd.all_execute(CLIENT_BUILD_PROJECT, -1, True)
            cmd.server_all_execute(KILL_SERVER)
            cmd.server_all_execute(UNSET_HUGEPAGE)
            cmd.server_all_execute(SERVER_BUILD_PROJECT, 0, -1, True)

            CN_num = len(client_ips)
            client_num_per_CN = 8
            
            for replica_num in index_replicas:
                SERVER_START = f"{server_env_cmd} && ./ycsb_test_server"
                if replica_num == 3:
                    MICRO_TEST = f"{client_env_cmd} && ./micro_test_tpt_multi_client ./client_config.json {client_num_per_CN} {CN_num}"
                else:
                    MICRO_TEST = f"{client_env_cmd} && ./micro_test_tpt_multi_client ./client_config_rep{replica_num}.json {client_num_per_CN} {CN_num}"
                op_tpts   = {}
                cas_tpts  = {}
                while True:
                    try:
                        cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
                        cmd.server_all_long_execute(SERVER_START, 0, MN_num)
                        cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
                        logs = cmd.all_long_execute(MICRO_TEST, CN_num)
                        op_tpts, cas_tpts = tp.get_op_and_cas_tpts(logs)
                        
                        cmd.all_execute(KILL_CLIENT, CN_num)
                        cmd.all_execute(UNSET_HUGEPAGE, CN_num)
                        cmd.server_all_execute(KILL_SERVER, 0, MN_num)
                        cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
                        break
                    except (FunctionTimedOut, Exception) as e:
                        print_WARNING(f"Error! Retry... {e}")

                print_GOOD(f"[FINISHED POINT] method={method} client_num={CN_num*client_num_per_CN}")
                
                for op in op_names:
                    plot_data['Y_data'][replica_num][op]     = op_tpts[op] / 10e5
                    plot_data['CAS_data'][replica_num][op]   = cas_tpts[op] / 10e5
        elif method == 'aceso':
            pass
        else:
            pass
                
    # save data
    Path(output_path).mkdir(exist_ok=True)
    with (Path(output_path) / f'fig_{fig_num}.json').open(mode='w') as f:
        json.dump(plot_data, f, indent=2)

if __name__ == '__main__':
    print_WARNING("remember to sudo su && remember first CN's server_id = 0")
    cmd = CMDManager(client_ips, master_ip, server_ips)
    tp = LogParser()
    t = main(cmd, tp)
