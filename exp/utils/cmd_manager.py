import paramiko
import time
import socket
from func_timeout import func_set_timeout
from typing import List

from utils.lat_parser import LatParser
from utils.color_printer import print_OK, print_FAIL, print_WARNING


BUFFER_SIZE = 16 * 1024 * 1024
END_PROMPT = '[END]'
OOM_PROMPT = 'shared memory space run out'
DEADLOCK_PROMPT = 'Deadlock'
ERROR_PROMPT1 = 'Error'
ERROR_PROMPT2 = 'error'
FAIL_PROMPT1 = 'Fail'
FAIL_PROMPT2 = 'fail'
MMERR_PROMPT = 'this->data_ == this->base_addr_'

CLOVER_MS_IP = '10.10.10.1'

class CMDManager(object):

    def __init__(self, client_ips: list, master_ip: str, server_ips: list):
        super().__init__()
        self.__client_ips = client_ips
        # self.__master_idx = client_ips.index(master_ip)
        self.__master_idx = 0
        self.__server_ips = server_ips
        self.__CNs = [self.__get_ssh_CNs(hostname) for hostname in client_ips]
        self.__MNs = [self.__get_ssh_CNs(hostname) for hostname in server_ips]
        self.__client_shells = [cli.invoke_shell() for cli in self.__CNs]
        self.__server_shells = [cli.invoke_shell() for cli in self.__MNs]
        for shell in self.__client_shells:
            shell.setblocking(False)
            self.__clear_shell(shell)
        for shell in self.__server_shells:
            shell.setblocking(False)
            self.__clear_shell(shell)
        self.__lat_parser = LatParser(self.__CNs)

    def __del__(self):
        for cli in self.__CNs:
            cli.close()

    def __clear_shell(self, shell):
        shell.send('\n')
        while True:
            try:
                shell.recv(BUFFER_SIZE)
                break
            except socket.timeout:
                continue

    def __match_prompt(self, content: str, end: str):
        if end in content:
            return True
        return False

    def __get_ssh_CNs(self, hostname: str):
        port = 22
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cli.connect(hostname, port, compress=True)
        return cli

    @func_set_timeout(30)
    def clear_all_shell(self):
        for shell in self.__client_shells:
            shell.send('\n')
            while shell.recv_ready():
                msg = shell.recv(BUFFER_SIZE).decode()
        for shell in self.__client_shells:
            shell.send('\n')
            while shell.recv_ready():
                msg = shell.recv(BUFFER_SIZE).decode()
        
        for shell in self.__server_shells:
            shell.send('\n')
            while shell.recv_ready():
                msg = shell.recv(BUFFER_SIZE).decode()
        for shell in self.__server_shells:
            shell.send('\n')
            while shell.recv_ready():
                msg = shell.recv(BUFFER_SIZE).decode()
                
    @func_set_timeout(30)
    def get_server_log(self):
        shell = self.__server_shells[0]
        shell.send('\n')
        logs = ''
        while shell.recv_ready():
            logs += shell.recv(BUFFER_SIZE).decode()
        logs = logs.strip().split('\n')
        return logs

    @func_set_timeout(60)
    def all_execute(self, command: str, CN_num: int = -1, is_silent: bool = False):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        outs = {}
        errs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__client_ips[:CN_num]}')
        print_WARNING(f'ONLY THE FIRST CLIENT\'S OUTPUT WILL BE PRINTED')
        for i in range(CN_num):
            cli_ip = self.__client_ips[i]
            _, stdouts[cli_ip], stderrs[cli_ip] = self.__CNs[i].exec_command(command, get_pty=True)
        for i in range(CN_num):
            cli_ip = self.__client_ips[i]
            outs[cli_ip] = stdouts[cli_ip].readlines()  # block here
            errs[cli_ip] = stderrs[cli_ip].readlines()  # TODO: Retry
            
            if i == 0:
                for line in outs[cli_ip]:
                    if is_silent == False:
                        print(f'[CN {cli_ip} OUTPUT] {line.strip()}')
                for line in errs[cli_ip]:
                    print_FAIL(f'[CN {cli_ip} ERROR] {line.strip()}')
        return outs

    @func_set_timeout(60)
    def all_execute_fast(self, command: str, CN_num: int = -1):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        outs = {}
        errs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__client_ips[:CN_num]}')
        print_WARNING(f'ONLY THE FIRST CLIENT\'S OUTPUT WILL BE PRINTED')
        for i in range(CN_num):
            cli_ip = self.__client_ips[i]
            _, stdouts[cli_ip], stderrs[cli_ip] = self.__CNs[i].exec_command(command, get_pty=True)
        return outs

    def one_execute(self, command: str):
        # one of the nodes (i.e., master node) will do some special task
        cli = self.__CNs[self.__master_idx]
        cli_ip = self.__client_ips[self.__master_idx]
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IP={cli_ip}')
        try:
            _, stdout, stderr = cli.exec_command(command, get_pty=True)
            out = stdout.readlines()  # block here
            err = stderr.readlines()
            for line in out:
                print(f'[CN {cli_ip} OUTPUT] {line.strip()}')
            for line in err:
                print_FAIL(f'[CN {cli_ip} OUTPUT] {line.strip()}')
        except:
            print_FAIL(f'[CN {cli_ip}] FAILURE: {command}')
        return out

    @func_set_timeout(60)
    def all_long_execute_fig8(self, command: str, CN_num: int = -1):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__client_ips[:CN_num]}')
        print_WARNING(f'ONLY THE FIRST CLIENT\'S OUTPUT WILL BE PRINTED')
        if not command.endswith('\n'):
            command += '\n'
        
        # drop last time's msgs
        for i in range(CN_num):
            while self.__client_shells[i].recv_ready():
                msg = self.__client_shells[i].recv(BUFFER_SIZE).decode()

        for i in range(CN_num):
            self.__client_shells[i].send(command)
        for i in range(CN_num):
            while not self.__client_shells[i].recv_ready():
                time.sleep(0.2)

        outs = {self.__client_ips[i]: '' for i in range(CN_num)}
        for i in range(CN_num):
            cli_ip = self.__client_ips[i]
            while not self.__match_prompt(outs[cli_ip], END_PROMPT):
                try:
                    msg = self.__client_shells[i].recv(BUFFER_SIZE).decode()
                    if msg:
                        if i == 0:
                            print(f'[CN {cli_ip} OUTPUT] {msg.strip()}')
                        outs[cli_ip] += msg
                    if self.__match_prompt(outs[cli_ip], 'load trans'):
                        return
                    if self.__match_prompt(outs[cli_ip], OOM_PROMPT):
                        raise Exception(OOM_PROMPT)
                    if self.__match_prompt(outs[cli_ip], DEADLOCK_PROMPT):
                        raise Exception(DEADLOCK_PROMPT)
                except socket.timeout:
                    continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs

    @func_set_timeout(360)
    def all_long_execute(self, command: str, CN_num: int = -1):
        if CN_num < 0:  # -1 means use all CNs
            CN_num = len(self.__CNs)
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__client_ips[:CN_num]}')
        print_WARNING(f'ONLY THE FIRST CLIENT\'S OUTPUT WILL BE PRINTED')
        if not command.endswith('\n'):
            command += '\n'
        for i in range(CN_num):
            self.__client_shells[i].send(command)
        for i in range(CN_num):
            while not self.__client_shells[i].recv_ready():
                time.sleep(0.2)

        outs = {self.__client_ips[i]: '' for i in range(CN_num)}
        for i in range(CN_num):
            cli_ip = self.__client_ips[i]
            while not self.__match_prompt(outs[cli_ip], END_PROMPT):
                try:
                    msg = self.__client_shells[i].recv(BUFFER_SIZE).decode()
                    if msg:
                        if i == 0:
                            print(f'[CN {cli_ip} OUTPUT] {msg.strip()}')
                        outs[cli_ip] += msg
                    if self.__match_prompt(outs[cli_ip], OOM_PROMPT):
                        raise Exception(OOM_PROMPT)
                    if self.__match_prompt(outs[cli_ip], DEADLOCK_PROMPT):
                        raise Exception(DEADLOCK_PROMPT)
                except socket.timeout:
                    continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs
    
    @func_set_timeout(60)
    def server_all_execute(self, command: str, MN_st_num: int = 0, MN_ed_num: int = -1, is_silent: bool = False):
        if MN_ed_num < 0:  # -1 means use all CNs
            MN_ed_num = len(self.__MNs)
        outs = {}
        errs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__server_ips[MN_st_num:MN_ed_num]}')
        print_WARNING(f'ONLY THE FIRST SERVER\'S OUTPUT WILL BE PRINTED')
        for i in range(MN_st_num, MN_ed_num):
            ser_ip = self.__server_ips[i]
            _, stdouts[ser_ip], stderrs[ser_ip] = self.__MNs[i].exec_command(command, get_pty=True)
        for i in range(MN_st_num, MN_ed_num):
            ser_ip = self.__server_ips[i]
            outs[ser_ip] = stdouts[ser_ip].readlines()  # block here
            errs[ser_ip] = stderrs[ser_ip].readlines()  # TODO: Retry
            if i == 0:
                for line in outs[ser_ip]:
                    if is_silent == False:
                        print(f'[MN {ser_ip} OUTPUT] {line.strip()}')
                for line in errs[ser_ip]:
                    print_FAIL(f'[MN {ser_ip} ERROR] {line.strip()}')
        return outs
    
    @func_set_timeout(60)
    def server_all_execute_fast(self, command: str, MN_st_num: int = 0, MN_ed_num: int = -1):
        if MN_ed_num < 0:  # -1 means use all CNs
            MN_ed_num = len(self.__MNs)
        outs = {}
        errs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__server_ips[MN_st_num:MN_ed_num]}')
        for i in range(MN_st_num, MN_ed_num):
            ser_ip = self.__server_ips[i]
            _, stdouts[ser_ip], stderrs[ser_ip] = self.__MNs[i].exec_command(command, get_pty=True)
        return stdouts

    @func_set_timeout(90)
    def server_all_long_execute(self, command: str, MN_st_num: int = 0, MN_ed_num: int = -1):
        if MN_ed_num < 0:  # -1 means use all CNs
            MN_ed_num = len(self.__MNs)
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__server_ips[MN_st_num:MN_ed_num]}')
        print_WARNING(f'ONLY THE FIRST SERVER\'S OUTPUT WILL BE PRINTED')
        if not command.endswith('\n'):
            command += '\n'
        for i in range(MN_st_num, MN_ed_num):
            self.__server_shells[i].send(command)
        for i in range(MN_st_num, MN_ed_num):
            while not self.__server_shells[i].recv_ready():
                time.sleep(0.2)
        
        outs = {self.__server_ips[i]: '' for i in range(MN_st_num, MN_ed_num)}
        for i in range(MN_st_num, MN_ed_num):
            ser_ip = self.__server_ips[i]
            while not self.__match_prompt(outs[ser_ip], END_PROMPT):
                try:
                    msg = self.__server_shells[i].recv(BUFFER_SIZE).decode()
                    if msg:
                        if i == 0:
                            print(f'[MN {ser_ip} OUTPUT] {msg.strip()}')
                        outs[ser_ip] += msg
                    if self.__match_prompt(outs[ser_ip], OOM_PROMPT):
                        raise Exception(OOM_PROMPT)
                    if self.__match_prompt(outs[ser_ip], DEADLOCK_PROMPT):
                        raise Exception(DEADLOCK_PROMPT)
                    if self.__match_prompt(outs[ser_ip], MMERR_PROMPT):
                        raise Exception(MMERR_PROMPT)
                except socket.timeout:
                    continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs
    
    def clover_meta_execute(self, command: str):
        return self.server_all_execute(command, 0, 1)
    
    @func_set_timeout(60)
    def clover_meta_execute_not_wait(self, command: str):
        outs = {}
        stdouts = {}
        stderrs = {}
        print_OK(f'COMMAND="{command}"')
        print_OK(f'EXECUTE_IPs={self.__server_ips[0]}')

        ser_ip = self.__server_ips[0]
        _, stdouts[ser_ip], stderrs[ser_ip] = self.__MNs[0].exec_command(command, get_pty=True)
        
        return outs
    
    def clover_memo_all_execute(self, command: str, MN_num: int = -1):
        if MN_num < 0:  # -1 means use all MNs
            MN_num = len(self.__MNs) - 1
        return self.server_all_execute(command, 1, 1 + MN_num)
    
    # specificly for running ./init
    @func_set_timeout(100)
    def clover_meta_start_exp(self, env_cmd, CN_num, MN_num, client_num_per_CN, bench_type, test_type,  ycsb_mode, upd_type):
        META_START = f"{env_cmd} && ./run_memcached.sh && ./init -S 1 -L 2 -I 0 -b 1 -d 0 -c {CN_num} -m {MN_num} -X {CLOVER_MS_IP} -t {client_num_per_CN} -B {bench_type} -T {test_type} -y {ycsb_mode} -u {upd_type}"
        ser_idx = 0
        ser_ip  = self.__server_ips[0]
        print_OK(f'COMMAND="{META_START}"')
        print_OK(f'EXECUTE_IPs={ser_ip}')
        if not META_START.endswith('\n'):
            META_START += '\n'
            
        # drop last time's msgs
        while self.__server_shells[ser_idx].recv_ready():
            msg = self.__server_shells[ser_idx].recv(BUFFER_SIZE).decode()
            
        self.__server_shells[ser_idx].send(META_START)
        while not self.__server_shells[ser_idx].recv_ready():
            time.sleep(0.2)
        
        outs = {self.__server_ips[ser_idx]: ''}
        while not self.__match_prompt(outs[ser_ip], END_PROMPT):
            try:
                msg = self.__server_shells[ser_idx].recv(BUFFER_SIZE).decode()
                if msg:
                    print(f'[MN {ser_ip} OUTPUT] {msg.strip()}')
                    outs[ser_ip] += msg
                if self.__match_prompt(outs[ser_ip], OOM_PROMPT):
                    raise Exception(OOM_PROMPT)
                if self.__match_prompt(outs[ser_ip], DEADLOCK_PROMPT):
                    raise Exception(DEADLOCK_PROMPT)
                if self.__match_prompt(outs[ser_ip], MMERR_PROMPT):
                    raise Exception(MMERR_PROMPT)
            except socket.timeout:
                continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs
        
    
    @func_set_timeout(100)
    def clover_memo_start_exp(self, env_cmd, CN_num, MN_num, client_num_per_CN, bench_type, test_type,  ycsb_mode, upd_type):
        for i in range(CN_num+1, CN_num+1+MN_num):
            ser_idx = i - CN_num
            ser_ip = self.__server_ips[ser_idx]
            
            # drop last time's msgs
            while self.__server_shells[ser_idx].recv_ready():
                msg = self.__server_shells[ser_idx].recv(BUFFER_SIZE).decode()
        
        for i in range(CN_num+1, CN_num+1+MN_num):
            MEMO_START = f"{env_cmd} && ./init -M 1 -L 2 -I {i} -b 1 -d 0 -c {CN_num} -m {MN_num} -X {CLOVER_MS_IP} -t {client_num_per_CN} -B {bench_type} -T {test_type} -y {ycsb_mode} -u {upd_type}"
            ser_idx = i - CN_num
            ser_ip = self.__server_ips[ser_idx]
            
            print_OK(f'COMMAND="{MEMO_START}"')
            print_OK(f'EXECUTE_IPs={ser_ip}')
            if not MEMO_START.endswith('\n'):
                MEMO_START += '\n'
        
            self.__server_shells[ser_idx].send(MEMO_START)
            
        for i in range(CN_num+1, CN_num+1+MN_num):
            ser_idx = i - CN_num
            while not self.__server_shells[ser_idx].recv_ready():
                time.sleep(0.2)
        
        outs = {self.__server_ips[i - CN_num]: '' for i in range(CN_num+1, CN_num+1+MN_num)}
        for i in range(CN_num+1, CN_num+1+MN_num):
            ser_idx = i - CN_num
            ser_ip = self.__server_ips[ser_idx]
            while not self.__match_prompt(outs[ser_ip], END_PROMPT):
                try:
                    msg = self.__server_shells[ser_idx].recv(BUFFER_SIZE).decode()
                    if msg:
                        print(f'[MN {ser_ip} OUTPUT] {msg.strip()}')
                        outs[ser_ip] += msg
                    if self.__match_prompt(outs[ser_ip], ERROR_PROMPT1):
                        raise Exception(ERROR_PROMPT1)
                    if self.__match_prompt(outs[ser_ip], ERROR_PROMPT2):
                        raise Exception(ERROR_PROMPT2)
                    if self.__match_prompt(outs[ser_ip], FAIL_PROMPT1):
                        raise Exception(FAIL_PROMPT1)
                    if self.__match_prompt(outs[ser_ip], FAIL_PROMPT2):
                        raise Exception(FAIL_PROMPT2)
                    if self.__match_prompt(outs[ser_ip], OOM_PROMPT):
                        raise Exception(OOM_PROMPT)
                    if self.__match_prompt(outs[ser_ip], DEADLOCK_PROMPT):
                        raise Exception(DEADLOCK_PROMPT)
                    if self.__match_prompt(outs[ser_ip], MMERR_PROMPT):
                        raise Exception(MMERR_PROMPT)
                except socket.timeout:
                    continue

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs
    
    @func_set_timeout(100)
    def clover_clie_start_exp(self, env_cmd, CN_num, MN_num, client_num_per_CN, bench_type, test_type,  ycsb_mode, upd_type):
        for i in range(1, CN_num+1):
            cli_idx = i - 1
            cli_ip = self.__client_ips[cli_idx]
            
            # drop last time's msgs
            while self.__client_shells[cli_idx].recv_ready():
                msg = self.__client_shells[cli_idx].recv(BUFFER_SIZE).decode()
        
        for i in range(1, CN_num+1):
            CLIE_START = f"{env_cmd} && ./init -C 1 -L 2 -I {i} -b 1 -d 0 -c {CN_num} -m {MN_num} -X {CLOVER_MS_IP} -t {client_num_per_CN} -B {bench_type} -T {test_type} -y {ycsb_mode} -u {upd_type}"
            cli_idx = i - 1
            cli_ip = self.__client_ips[cli_idx]
            
            print_OK(f'COMMAND="{CLIE_START}"')
            print_OK(f'EXECUTE_IPs={cli_ip}')
            if not CLIE_START.endswith('\n'):
                CLIE_START += '\n'
        
            self.__client_shells[cli_idx].send(CLIE_START)
            
        for i in range(1, CN_num+1):
            cli_idx = i - 1
            while not self.__client_shells[cli_idx].recv_ready():
                time.sleep(0.2)
        
        outs = {self.__client_ips[i - 1]: '' for i in range(1, CN_num+1)}
        for i in range(1, CN_num+1):
            cli_idx = i - 1
            cli_ip = self.__client_ips[cli_idx]
            ms_ip = self.__server_ips[0]
            while not self.__match_prompt(outs[cli_ip], END_PROMPT):
                if self.__client_shells[cli_idx].recv_ready():
                    try:
                        msg = self.__client_shells[cli_idx].recv(BUFFER_SIZE).decode()
                        if msg:
                            print(f'[MN {cli_ip} OUTPUT] {msg.strip()}')
                            outs[cli_ip] += msg
                        if self.__match_prompt(outs[cli_ip], ERROR_PROMPT1):
                            raise Exception(ERROR_PROMPT1)
                        if self.__match_prompt(outs[cli_ip], ERROR_PROMPT2):
                            raise Exception(ERROR_PROMPT2)
                        if self.__match_prompt(outs[cli_ip], FAIL_PROMPT1):
                            raise Exception(FAIL_PROMPT1)
                        if self.__match_prompt(outs[cli_ip], FAIL_PROMPT2):
                            raise Exception(FAIL_PROMPT2)
                        if self.__match_prompt(outs[cli_ip], OOM_PROMPT):
                            raise Exception(OOM_PROMPT)
                        if self.__match_prompt(outs[cli_ip], DEADLOCK_PROMPT):
                            raise Exception(DEADLOCK_PROMPT)
                        if self.__match_prompt(outs[cli_ip], MMERR_PROMPT):
                            raise Exception(MMERR_PROMPT)
                    except socket.timeout:
                        continue
                # check ms's status
                if self.__server_shells[0].recv_ready():
                    try:
                        ms_msg = self.__server_shells[0].recv(BUFFER_SIZE).decode()
                        if ms_msg:
                            print(f'[MN {ms_ip} OUTPUT] {ms_msg.strip()}')
                        if self.__match_prompt(ms_msg, ERROR_PROMPT1):
                            raise Exception(ERROR_PROMPT1)
                        if self.__match_prompt(ms_msg, ERROR_PROMPT2):
                            raise Exception(ERROR_PROMPT2)
                        if self.__match_prompt(ms_msg, FAIL_PROMPT1):
                            raise Exception(FAIL_PROMPT1)
                        if self.__match_prompt(ms_msg, FAIL_PROMPT2):
                            raise Exception(FAIL_PROMPT2)
                        if self.__match_prompt(ms_msg, OOM_PROMPT):
                            raise Exception(OOM_PROMPT)
                        if self.__match_prompt(ms_msg, DEADLOCK_PROMPT):
                            raise Exception(DEADLOCK_PROMPT)
                        if self.__match_prompt(ms_msg, MMERR_PROMPT):
                            raise Exception(MMERR_PROMPT)
                    except socket.timeout:
                        continue
                    

        for ip in outs.keys():
            outs[ip] = outs[ip].strip().split('\n')
        return outs

    def get_cluster_lats(self, type: str, lat_dir_path: str, op_names: List[str], CN_num: int, client_num_per_CN: int, coro_num_per_client: int):
        op_p50_p99_lats = self.__lat_parser.load_remote_lats(type, lat_dir_path, op_names, CN_num, client_num_per_CN, coro_num_per_client)
        return op_p50_p99_lats

    def get_crash_tpts(self, tpt_dir_path: str, CN_num: int, client_num_per_CN: int):
        search_tpts = self.__lat_parser.load_remote_crash_tpts(tpt_dir_path, CN_num, client_num_per_CN)
        return search_tpts
