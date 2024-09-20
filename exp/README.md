# Reproduce All Experiment Results

In this folder, we provide code and scripts for reproducing figures in our paper.
The name of each script corresponds to the number of each figure in the accepted version of our paper (on the artifact submission site).

## Prepare the baseline

> **!!! Skip this step if you are using our created cluster with the provided account.**

We use [FUSEE](https://github.com/dmemsys/FUSEE) (FAST'23) as the baseline in our paper. We have forked FUSEE and modified it to fit our reproduction scripts. 

1. Use the following command to clone the [forked FUSEE](https://github.com/huzhisheng/Aceso-FUSEE) in the same path of Aceso (*i.e.*, the home directory) in each node:
    ```shell
    cd ~/home
    git clone https://github.com/huzhisheng/Aceso-FUSEE.git fusee
    ```

2. Build FUSEE.
    ```shell
    cd ~/home/fusee
    mkdir build && cd build
    cmake .. && make -j
    ```

3. Set the `server_config.json` and `client_config.json` in its `./build/micro-test` and `./build/ycsb-test` directories (See FUSEE's README). We have prepared the example `server_config.json` and `client_config.json` files in the `./test-config` direcotry.
    ```shell
    cd ~/home/fusee/test-config
    <modify these config files> # especially the `server_id` and `memory_ips` attributes
    
    cd ~/home/fusee
    cp ./test-config/* ./build/micro-test
    cp ./test-config/* ./build/ycsb-test
    ```

    On CNs, we will use `client_config.json`, `client_config_rep2.json`, and `client_config_rep1.json`. On MNs, we will use `server_config.json`.

    Note that, in FUSEE, the `server_id` parameter of the `i-th` CN should be set to `5 + i * 8`. For example, the `server_id` of the first three CNs are 5, 13, 21 respectively.

4. Copy macrobenchmark workloads from Aceso to FUSEE.
    ```shell
    mkdir -p ~/home/fusee/build/ycsb-test/workloads/
    # This takes about 5 minutes
    cp ~/home/aceso/build/major-test/workloads/* ~/home/fusee/build/ycsb-test/workloads/
    ```

5. Try FUSEE.
    ```shell
    # On MNs
    cd ~/home/fusee/build/ycsb-test && ./ycsb_test_server
    ```

    ```shell
    # On CNs
    cd ~/home/fusee/build/micro-test && ./micro_test_tpt_multi_client ./client_config.json <client_num_per_CN> <CN_num>
    ```

## Setup the experiment parameter

> **!!! Skip this step if you are using our created cluster with the provided account.**

1. Change the `home_dir` value in `./params/common.json` to your actual home directory path (*i.e.*, /users/XXX/home).

2. Change the `server_ips` value in `./params/common.json` to the actual IPs of MNs. 

    Note that, in previous steps, we have used `setup-ipoib.sh` in `./setup` to allocate a RDMA IP `10.10.10.x`  from the ethernet IP `10.10.1.x`.

3. Change the `client_ips` value in `./params/common.json` to the actual IPs of CNs.

4. Change the `master_ip` value in `./params/common.json` to the actual IP of the first node.

*Example*:
```json
"home_dir" : "/users/hzs/home",

"server_ips": ["10.10.10.1", "10.10.10.2", "10.10.10.3", "10.10.10.4", "10.10.10.5"],
"client_ips": ["10.10.10.6", "10.10.10.7", "10.10.10.8", "10.10.10.9", "10.10.10.10", "10.10.10.11", "10.10.10.12", "10.10.10.13", "10.10.10.14", "10.10.10.15", "10.10.10.16", "10.10.10.17", "10.10.10.18", "10.10.10.19", "10.10.10.20"],
"master_ip": "10.10.10.1",
```


## Start to run

* In the c6220&r320 cluster, the node with ip `10.10.1.1` (with host name `node-0`) can directly establish SSH connections to other nodes. Thus we define it as the **master** node and run our scripts on it. Each script can reproduce the corresponding results and figures in our paper.

    Note that when using these scripts, please skip all the steps in previous **Step-3 Try Aceso** (*e.g.*, setting hugepages, running memcached on the MNs, and starting MN servers) and run the script directly, as these steps are already included.

*   You can run all the scripts with a single batch script on the master node using the following command:

    ```shell
    # On the ip `10.10.1.1` node only
    cd ~/home/aceso/exp
    sudo su
    # This takes about 180 minutes
    sh run_all.sh
    ```

    Or, you can run the scripts one by one, or run some specific scripts if some figures are skipped or show unexpected results during `run_all.sh` (due to network instability, which happens sometimes):

    ```shell
    # On the ip `10.10.1.1` node only
    cd ~/home/aceso/exp
    sudo su
    # This takes about 3 minutes
    sh fig_1a.sh
    # This takes about 11 minutes
    sh fig_1b.sh
    # This takes about 4 minutes
    sh fig_8.sh
    # This takes about 4 minutes
    sh fig_9.sh
    # This takes about 5 minutes
    sh fig_10.sh
    # This takes about 13 minutes
    sh fig_11.sh
    # This takes about 3 minutes
    sh fig_12.sh
    # This takes about 10 minutes
    sh fig_13.sh
    # This takes about 4 minutes
    sh fig_14.sh
    # This takes about 19 minutes
    sh fig_15.sh
    # This takes about 10 minutes
    sh fig_16.sh
    # This takes about 35 minutes
    sh fig_17.sh
    # This takes about 16 minutes
    sh fig_18.sh
    # This takes about 4 minutes
    sh fig_19.sh
    # This takes about 31 minutes
    sh fig_20.sh
    # This takes about 3 minutes
    sh tab_2.sh
    ```

* The json results will be stored in `./exp/results`, and PDF figures will be stored in `./exp/figures`.

    The results you get may not be exactly the same as the ones shown in the paper due to changes of physical machines.

    - We use 5 c6220 nodes as MNs and 23 r320 nodes as CNs in our paper. However, currently, we can only successfully rent a maximum of 6 c6220 nodes and 14 r320 nodes that can interconnect for the experiment. As a result, the SEARCH throughput of FUSEE and Aceso will be lower.

    - Some results may fluctuate due to the instability of RNICs in the cluster.

    However, all results here support the conclusions we made in the paper.
