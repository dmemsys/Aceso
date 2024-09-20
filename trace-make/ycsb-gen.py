import sys
import os
# [install ycsb first]
# curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
# tar xfvz ycsb-0.17.0.tar.gz
# cd ycsb-0.17.0
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

if (len(sys.argv) != 2) :
    print(bcolors.WARNING + 'Usage:')
    print('python3 ycsb-gen.py workload_name' + bcolors.ENDC)
    exit(0)

workload_name = sys.argv[1]

ycsb_dir = '~/home/ycsb-0.17.0/' # where we install ycsb
workload_dir = 'workload-spec/'
output_dir= 'workloads/'

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

print(bcolors.OKGREEN + 'workload = ' + workload_name + bcolors.ENDC)



temp_load_file = output_dir + workload_name + '.spec_load.tmp'
temp_trans_file = output_dir + workload_name + '.spec_trans.tmp'
out_load_file = output_dir + workload_name + '.spec_load'
out_trans_file = output_dir + workload_name + '.spec_trans'


cmd_ycsb_load = ycsb_dir + 'bin/ycsb load basic -P ' + workload_dir + workload_name + ' -s > ' + temp_load_file
cmd_ycsb_txn = ycsb_dir + 'bin/ycsb run basic -P ' + workload_dir + workload_name + ' -s > ' + temp_trans_file

os.system(cmd_ycsb_load)
os.system(cmd_ycsb_txn)

#####################################################################################

f_load = open (temp_load_file, 'r')
f_load_out = open (out_load_file, 'w')
for line in f_load :
    cols = line.split()
    if len(cols) > 0 and cols[0] == "INSERT":
        f_load_out.write (cols[0] + " " + cols[1] + " " + cols[2] + "\n")
f_load.close()
f_load_out.close()

new_insert_keys = set()
last_req = None
f_txn = open (temp_trans_file, 'r')
f_txn_out = open (out_trans_file, 'w')
for line in f_txn :
    cols = line.split()
    if (cols[0] == 'INSERT'):
        f_txn_out.write (cols[0] + " " + cols[1] + " " + cols[2] + "\n")
        new_insert_keys.add(cols[2])
    elif (cols[0] == 'READ') or (cols[0] == 'UPDATE'):
        if (cols[2] not in new_insert_keys):
            f_txn_out.write (cols[0] + " " + cols[1] + " " + cols[2] + "\n")
            last_req = cols[0] + " " + cols[1] + " " + cols[2] + "\n"
        else:
            f_txn_out.write (last_req)
f_txn.close()
f_txn_out.close()

cmd = 'rm -f ' + temp_load_file
os.system(cmd)
cmd = 'rm -f ' + temp_trans_file
os.system(cmd)