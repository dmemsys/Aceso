# MN's 4 cpu usage ratio test (MN's id = 1)
# currently unused
from func_timeout import FunctionTimedOut
from pathlib import Path
import json
import time
from utils.cmd_manager import CMDManager
from utils.log_parser import LogParser
from utils.color_printer import print_GOOD, print_WARNING, print_OK
from utils.func_timer import print_func_time

input_path = './params'
style_path = "./styles"
output_path = './results'
fig_num = 'tab_3'
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
with (Path(input_path) / f'{fig_num}.json').open(mode='r') as f:
    fig_params = json.load(f)
MN_num             = fig_params['MN_num']


# test params
method = 'aceso'

project_dir = f"{home_dir}/{method}"
server_work_dir = f"{project_dir}/build/major-test"
monito_work_dir = f"{project_dir}/ycsb-test"
client_work_dir = f"{project_dir}/build/major-test"
server_env_cmd = f"cd {server_work_dir}"
client_env_cmd = f"cd {client_work_dir}"
monito_env_cmd = f"cd {monito_work_dir}"

# only for aceso
memcache_dir = f"{project_dir}/src"
memcache_env_cmd = f"cd {memcache_dir}"

cmake_option = "-DENABLE_INDEX_SIZE_256=on -DENABLE_RPC_BLOCK=on"
SERVER_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd major-test && mkdir -p results"
CLIENT_BUILD_PROJECT = f"cd {project_dir} && mkdir -p build && cd build && rm -f CMakeCache.txt && cmake {cmake_option} .. && make clean && make -j && cd major-test && mkdir -p results"
SET_CN_HUGEPAGE = "echo 4000 | sudo tee /proc/sys/vm/nr_hugepages"
SET_MN_HUGEPAGE = "echo 28000 | sudo tee /proc/sys/vm/nr_hugepages"
UNSET_HUGEPAGE = "echo 0 | sudo tee /proc/sys/vm/nr_hugepages"
KILL_SERVER = f"{server_env_cmd} && killall -9 server"
KILL_CLIENT = f"{client_env_cmd} && killall -9 client_perf"

CN_num = len(client_ips)
client_num_per_CN = 8

SERVER_START = f"{memcache_env_cmd} && ./run_memcached.sh && {server_env_cmd} && ./server"
MICRO_TEST = f"{client_env_cmd} && ./client_perf update-consume {CN_num} {client_num_per_CN} 8"

@print_func_time
def main(cmd: CMDManager, tp: LogParser):
    test_prepare(cmd, tp)
    server_cpu_monitor_start()
    test_start(cmd, tp)


def test_prepare(cmd: CMDManager, tp: LogParser):
    cmd.all_execute(KILL_CLIENT)
    cmd.all_execute(UNSET_HUGEPAGE)
    cmd.all_execute(CLIENT_BUILD_PROJECT)
    cmd.server_all_execute(KILL_SERVER)
    cmd.server_all_execute(UNSET_HUGEPAGE)
    cmd.server_all_execute(SERVER_BUILD_PROJECT)
    try:
        cmd.server_all_execute(SET_MN_HUGEPAGE, 0, MN_num)
        cmd.server_all_long_execute(SERVER_START, 0, MN_num)
        cmd.all_execute(SET_CN_HUGEPAGE, CN_num)
    except (FunctionTimedOut, Exception) as e:
        print_WARNING(f"Error! {e}")

def server_cpu_monitor_start():
    MONITOR_START = f"{monito_env_cmd} && bash ./server_cpu_monitor.sh"
    cmd.server_all_execute(MONITOR_START, 0, MN_num)

def test_start(cmd: CMDManager, tp: LogParser):
    try:
        cmd.all_long_execute_fig8(MICRO_TEST, CN_num)
        time.sleep(20)
        cmd.all_execute(KILL_CLIENT, CN_num)
        cmd.all_execute(UNSET_HUGEPAGE, CN_num)
        cmd.server_all_execute(KILL_SERVER, 0, MN_num)
        cmd.server_all_execute(UNSET_HUGEPAGE, 0, MN_num)
    except (FunctionTimedOut, Exception) as e:
        print_WARNING(f"Error! {e}")
    print_GOOD(f"[FINISHED POINT] method={method} client_num={CN_num*client_num_per_CN}")                

if __name__ == '__main__':
    print_WARNING("remember to sudo su && remember first CN's server_id = 0")
    cmd = CMDManager(client_ips, master_ip, server_ips)
    tp = LogParser()
    t = main(cmd, tp)
    print_OK(f"[WAIT MONITOR]")
    time.sleep(30)
