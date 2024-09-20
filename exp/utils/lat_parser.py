from typing import List
from pathlib import Path
import paramiko
from utils.color_printer import print_OK

class LatParser(object):

    def __init__(self, clients: List[paramiko.SSHClient]):
        self.__sftps = [cli.open_sftp() for cli in clients]
        self.__lat_cnt = dict()


    def load_remote_lats(self, type: str, lat_dir_path: str, op_names: List[str], CN_num: int, client_num_per_CN: int, coro_num_per_client: int):
        op_p50_p99_lats = {}
        for op in op_names:
            self.__lat_cnt.clear()
            for sftp in self.__sftps[:CN_num]:
                remote_file = sftp.open(str(Path(lat_dir_path) / f'{type}_{op}_lat.txt'))
                try:
                    for line in remote_file:
                        lat, cnt = line.strip().split(' ', 1)
                        if int(cnt):
                            if lat not in self.__lat_cnt:
                                self.__lat_cnt[lat] = 0
                            self.__lat_cnt[lat] += int(cnt)
                finally:
                    remote_file.close()
            if self.__lat_cnt:
                op_p50_p99_lats[op] = self.__cal_lat()
                print_OK(f'op_name={op} p50_lat={op_p50_p99_lats[op][0]} p99_lat={op_p50_p99_lats[op][1]}')
        return op_p50_p99_lats

    def load_remote_crash_tpts(self, tpt_dir_path: str, CN_num: int, client_num_per_CN: int):
        search_tpts = []
        accumulative_tpts = {}
        max_i = 0
        for sftp in self.__sftps[:CN_num]:
            for i in range(client_num_per_CN):
                remote_file = sftp.open(str(Path(tpt_dir_path) / f'{i}-tpt-vec-on-crash.txt'))
                try:
                    for i, line in enumerate(remote_file):
                        if i > max_i:
                            max_i = i
                        total_tpt = line.strip().split(' ')[0]
                        if i not in accumulative_tpts:
                            accumulative_tpts[i] = 0
                        accumulative_tpts[i] += int(total_tpt)
                finally:
                    remote_file.close()
        last_accumulative_tpt = 0
        for i in range(max_i + 1):
            cur_accumulative_tpt = accumulative_tpts[i]
            search_tpts.append(cur_accumulative_tpt - last_accumulative_tpt)
            last_accumulative_tpt = cur_accumulative_tpt
        return search_tpts


    def __cal_lat(self):
        all_lat = sum(self.__lat_cnt.values())
        th50 = all_lat / 2
        th99 = all_lat * 99 / 100
        cum = 0
        p50, p99 = None, None
        for lat, cnt in sorted(self.__lat_cnt.items(), key=lambda s:float(s[0])):
            cum += cnt
            if cum >= th50:
                p50 = float(lat)
                th50 = all_lat + 1
            if cum >= th99:
                p99 = float(lat)
                break
        assert(p50 is not None and p99 is not None)
        return p50, p99

