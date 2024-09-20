from typing import Optional

from utils.color_printer import print_FAIL


class LogParser(object):

    def __init__(self):
        pass
    
    def get_recover_time(self, logs: dict):
        recover_time_dict = {}
        for log in logs.values():
            for line in log:
                if "meta recover" in line:
                    recover_time_dict['meta'] = int(line.strip().split(' ')[-1])
                    # recover_time_list.append(int(line.strip().split(' ')[2]))
                if "hash recover" in line:
                    recover_time_dict['hash'] = int(line.strip().split(' ')[-1])
                if "data recover" in line:
                    recover_time_dict['data'] = int(line.strip().split(' ')[-1])
                    recover_time_dict['data-block-count'] = int(line.strip().split(' ')[-2])
                if "index-old recover" in line:
                    recover_time_dict['index-old'] = int(line.strip().split(' ')[-1])
                if "index-lblock recover" in line:
                    recover_time_dict['index-lblock'] = int(line.strip().split(' ')[-1])
                    recover_time_dict['index-lblock-count'] = int(line.strip().split(' ')[-2])
                if "index-rblock recover" in line:
                    recover_time_dict['index-rblock'] = int(line.strip().split(' ')[-1])
                    recover_time_dict['index-rblock-count'] = int(line.strip().split(' ')[-2])
                if "index-check recover" in line:
                    recover_time_dict['index-check'] = int(line.strip().split(' ')[-1])
                if "check count" in line:
                    recover_time_dict['check-count'] = int(line.strip().split(' ')[-1])
        return recover_time_dict
    
    def get_ckpt_time(self, log):
        ckpt_dict = {}
        for line in log:
            if "Index Size(B)" in line:
                ckpt_dict['raw-size'] = int(line.strip().split(' ')[-1])
            if "Compress Size(B)" in line:
                ckpt_dict['size'] = int(line.strip().split(' ')[-1])
            if "Copy&XOR Time(us)" in line:
                ckpt_dict['copy-xor'] = int(line.strip().split(' ')[-1])
            if "Compress Time(us)" in line:
                ckpt_dict['compress'] = int(line.strip().split(' ')[-1])
            if "Decompress Time(us)" in line:
                ckpt_dict['decompress'] = int(line.strip().split(' ')[-1])
            if "XOR Time(us)" in line:
                ckpt_dict['xor'] = int(line.strip().split(' ')[-1])
        return ckpt_dict
    
    def get_consumption(self, logs: dict):
        cur_sz = 0
        raw_sz = 0
        for log in logs.values():
            for line in log:
                if "cur_valid_kv_sz" in line:
                    cur_sz += int(line.strip().split(' ')[1])
                if "raw_valid_kv_sz" in line:
                    raw_sz += int(line.strip().split(' ')[1])
        return (cur_sz, raw_sz)
    
    def get_total_tpt(self, logs: dict):
        total_tpt = 0
        for log in logs.values():
            tpt = self.__parse_log_tpt(log)
            total_tpt += tpt
        return total_tpt
    
    def get_total_tpt_cas(self, logs: dict):
        total_tpt = 0
        total_cas = 0
        for log in logs.values():
            tpt = self.__parse_log_tpt(log)
            cas = self.__parse_log_cas(log)
            total_tpt += tpt
            total_cas += cas
        return total_cas, total_tpt

    def get_op_tpts(self, logs: dict):
        op_tpts = {}
        insert_tpt = 0
        update_tpt = 0
        search_tpt = 0
        delete_tpt = 0
        for log in logs.values():
            for line in log:
                if "insert tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    insert_tpt += tpt
                elif "update tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    update_tpt += tpt
                elif "search tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    search_tpt += tpt
                elif "delete tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    delete_tpt += tpt
        op_tpts['insert'] = insert_tpt
        op_tpts['update'] = update_tpt
        op_tpts['search'] = search_tpt
        op_tpts['delete'] = delete_tpt
        return op_tpts

    def get_op_and_cas_tpts(self, logs: dict):
        op_tpts = {}
        insert_tpt = 0
        update_tpt = 0
        search_tpt = 0
        delete_tpt = 0
                
        cas_tpts = {}
        insert_cas = 0
        update_cas = 0
        search_cas = 0
        delete_cas = 0
        for log in logs.values():
            for line in log:
                if "insert tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    insert_tpt += tpt
                elif "update tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    update_tpt += tpt
                elif "search tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    search_tpt += tpt
                elif "delete tpt" in line:
                    tpt = int(line.strip().split(' ')[2])
                    delete_tpt += tpt
                
                elif "insert cas" in line:
                    tpt = int(line.strip().split(' ')[2])
                    insert_cas += tpt
                elif "update cas" in line:
                    tpt = int(line.strip().split(' ')[2])
                    update_cas += tpt
                elif "search cas" in line:
                    tpt = int(line.strip().split(' ')[2])
                    search_cas += tpt
                elif "delete cas" in line:
                    tpt = int(line.strip().split(' ')[2])
                    delete_cas += tpt
        op_tpts['insert'] = insert_tpt
        op_tpts['update'] = update_tpt
        op_tpts['search'] = search_tpt
        op_tpts['delete'] = delete_tpt

        cas_tpts['insert'] = insert_cas
        cas_tpts['update'] = update_cas
        cas_tpts['search'] = search_cas
        cas_tpts['delete'] = delete_cas
        return op_tpts, cas_tpts
        
    def __parse_log_tpt(self, log):
        tpt = None
        for line in log:
            if "total tpt" in line:
                tpt = int(line.strip().split(' ')[2])
                break
        assert(tpt is not None)
        return tpt
    
    def __parse_log_cas(self, log):
        cas = None
        for line in log:
            if "total cas" in line:
                cas = int(line.strip().split(' ')[2])
                break
        assert(cas is not None)
        return cas

    def get_statistics(self, logs: dict, target_epoch: int, get_avg: bool=False):
        for log in logs.values():
            if get_avg:
                tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = self.__parse_log_avg(log, target_epoch)
            else:
                tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = self.__parse_log(log, target_epoch)
            if tpt is not None:
                break
        assert(tpt is not None)
        return tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate

    def __parse_log(self, log, target_epoch):
        tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = None, 0, 0, 0
        flag = False
        for line in log:
            if f"epoch {target_epoch} passed!" in line:
                flag = True

            elif flag and "cluster throughput" in line:
                tpt = float(line.strip().split(' ')[2])

            elif flag and "cache hit rate" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    cache_hit_rate = float(data)

            elif flag and "avg. lock/cas fail cnt" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    lock_fail_cnt = float(data)

            elif flag and "read invalid leaf rate" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    leaf_invalid_rate = float(data) * 100
                break

        return tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate

    def __parse_log_avg(self, log, target_epoch):
        tpt, cache_hit_rate, lock_fail_cnt, leaf_invalid_rate = [], [], [], []
        flag = False
        start_epoch = max(target_epoch // 2, target_epoch - 4)
        cnt = start_epoch
        for line in log:
            if f"epoch {start_epoch} passed!" in line:
                flag = True

            elif flag and "cluster throughput" in line:
                tpt.append(float(line.strip().split(' ')[2]))

            elif flag and "cache hit rate" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    cache_hit_rate.append(float(data))

            elif flag and "avg. lock/cas fail cnt" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    lock_fail_cnt.append(float(data))

            elif flag and "read invalid leaf rate" in line:
                data = line.strip().split(' ')[4]
                if data.replace('.', '').isdigit():
                    leaf_invalid_rate.append(float(data) * 100)
                cnt += 1
                if cnt == target_epoch:
                    break

        def get_avg(l):
            return sum(l) / len(l) if l else 0

        return get_avg(tpt), get_avg(cache_hit_rate), get_avg(lock_fail_cnt), get_avg(leaf_invalid_rate)

    def get_redundant_statistics(self, log):
        redundant_read, redundant_write, redundant_cas = None, None, None
        flag = False
        for line in log:
            if "Calculation done!" in line:
                flag = True

            elif flag and "Avg. redundant rdma_read" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_read = float(data)

            elif flag and "Avg. redundant rdma_write" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_write = float(data)

            elif flag and "Avg. redundant rdma_cas" in line:
                data = line.strip().split(' ')[3]
                if data.replace('.', '').isdigit():
                    redundant_cas = float(data)
                break

        return redundant_read, redundant_write, redundant_cas
