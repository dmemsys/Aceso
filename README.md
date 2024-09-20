# Aceso: Achieving Efficient Fault Tolerance in Memory-Disaggregated Key-Value Stores

This is the implementation repository of our *SOSP'24* paper: **Aceso: Achieving Efficient Fault Tolerance in Memory-Disaggregated Key-Value Stores**.

This artifact provides the source code of **Aceso** and scripts to reproduce the experiment results.

This README is specifically for artifact evaluation (AE).

## For AE reviewers

Aceso's current implementation requires at least 6 nodes, with 5 of them set up as memory nodes (MNs) and the remaining as compute nodes (CNs).

We recommend running Aceso using c6220 instances on [CloudLab](https://www.cloudlab.us/) as MNs and r320 instances as CNs, where the code has been thoroughly tested.

We have reserved 6 c6220 nodes and 14 r320 nodes on CloudLab **from Aug 19th to Sep 9th** for AE, with the first 5 c6220 nodes as MNs and the remaining as CNs.

Please coordinate among all AE reviewers to ensure that only one person uses the cluster at a time, or the scripts may fail to run.

* You can simply use the ***provided account*** to use our reserved c6220&r320 nodes on CloudLab.
  * We have provided our account (username: `hzs`) on the artifact submission site. Contact us if you don't know the password.
  * Log into the provided account on CloudLab, then please submit the SSH public key of your personal computer via `hzs`|-->`Manage SSH keys`.
  * You will see an experiment named `Aceso-AE` after that from `Experiments`|-->`My Experiments`. 
  * Reboot all nodes in the cluster to have your submitted public key loaded via `Aceso-AE`|-->`List View`|-->`Reboot Selected` (This takes about 5 minutes).
  * Now you can log into all the 20 nodes with the `SSH command` in `List View`. If you find some nodes have broken shells (which happens sometimes after rebooting in CloudLab), you can reboot them again via `List View`|-->`Reboot Selected`.

## Step-1 Create a cluster on CloudLab

> **!!! Skip this step if you are using our created cluster with the provided account.**

1. Click [CloudLab](https://www.cloudlab.us/) and log in.

2. Click `Experiments`|-->`Create Experiment Profile`|-->`Upload File`.
Upload `./setup/cloudlab.profile` provided in this repo.

3. (Optional) Click `Edit Topology` to customize the cluster structure (*e.g.*, reduce the number of nodes).
Click on the cluster's central node in the topology to customize IP addresses for each node, ensuring they are in the form of `10.10.1.x`. 
Click `Accept` to save the topology for this profile.

4. Click `Create` to save this profile in your account.

5. Click `Instantiate` to create a cluster using the profile.

6. Try logging into and check each node using the SSH commands provided in the `List View`. 

## Step-2 Setup the environment *(Artifacts Available)*

> **!!! Skip this step if you are using our created cluster with the provided account.**

Note that we should run the following steps on **all** nodes we have created.

1. Log into a node. 

2. Copy the source code of `./setup/setup-sda4.sh` from GitHub to a new file in the node `~/setup-sda4.sh`.
    ```shell
    cd ~ 
    vim setup-sda4.sh
    <do the copy>
    ```

3. Mount the disk `sda4` to `~/home` directory.
    Currently the `~` directory has very limited space, so we need to mount the unallocated disk `sda4` to the `~/home` directory. 
    ```shell
    cd ~ 
    # Dont use `sudo`
    bash setup-sda4.sh
    ```

4. Download Aceso's repo and name it as `aceso`.
    ```shell
    cd ~/home
    git clone https://github.com/dmemsys/Aceso.git aceso
    ```

5. Enter the Aceso directory. Install libraries and tools.
    ```shell
    cd ~/home/aceso/setup
    # This takes about 30 minutes
    sudo bash setup-env.sh
    # This takes about 6 minutes
    sudo reboot

    cd ~/home/aceso/setup
    sudo bash setup-pylibs.sh
    # allocate an RDMA IP `10.10.10.x`  from the ethernet IP `10.10.1.x`.
    sudo bash setup-ipoib.sh
    sudo reboot
    ```
6. Check if the RNIC is working, and discard all nodes that are not working. 

    Unfortunately, we sometimes find that 1 or 2 nodes have their RNIC disabled, probably due to physical issues.

    ```shell
    ibstat
    # if RNIC is working it will show:
    # Port 1:
    #       State: Active
    #       Physical state: LinkUp
    #       Rate: 56
    ```

7. Modify Aceso `Common.h` in `./src`. Make sure parameters `memoryNodeNum`, `memoryIPs`, `memcachedIP` are correct.
    *Example*:
    ```cpp
    constexpr uint32_t memoryNodeNum    = 5;        // [CONFIG]
    constexpr char memoryIPs[16][16] = {            // [CONFIG]
      "10.10.10.1",
      "10.10.10.2",
      "10.10.10.3",
      "10.10.10.4",
      "10.10.10.5",
    };
    constexpr char memcachedIP[16] = "10.10.10.1";  // [CONFIG]
    ```

8. Build Aceso.
    
    ```shell
    cd ~/home/aceso && mkdir build && cd build
    cmake .. && make -j
    ```

9. Set Aceso ``server_id`` for each node.
    
    ```shell
    cd ~/home/aceso/build/major-test
    vim config.json
    ```

    Modify `config.json` to the following.

    ```json
    {
        "server_id": 0
    }
    ```

    Note that the `server_id` parameter of the `i-th` node should be `i`. It should start from 0 and be continuous.

## Step-3 Try Aceso *(Artifacts Functional)*

### On MNs

1. Set huge pages.
    ```shell
    # on each memory node (MN)
    echo 28000 | sudo tee /proc/sys/vm/nr_hugepages
    ```

2. Start servers.
    ```shell
    # We use memcached for: 
    #   1. RDMA initialization;
    #   2. synchronization of clients during experiments;
    
    # on each memory node (MN)
    cd ~/home/aceso/src && ./run_memcached.sh
    cd ~/home/aceso/build/major-test && ./server
    ```

### On CNs

1. Set huge pages.

    ```shell
    # on each compute node (CN)
    echo 4000 | sudo tee /proc/sys/vm/nr_hugepages
    ```

2. Start clients.

    ```shell
    # on each compute node (CN)
    cd ~/home/aceso/build/major-test && ./client_perf <workload_name> <CN_num> <client_num_per_CN> <coro_num_per_client>
    ```
    - workload_name: the name of workload to test.
        - microbenchmarks: insert, update, search, delete.
        - macrobenchmarks: workload[a|b|c|d], workloadupd[0|10|...100], workloadtwi[s|c|t].
    - CN_num: the number of compute nodes (CNs).
    - client_num_per_CN: the number of clients in each CN.
    - coro_num_per_client: the number of coroutines in each client.

    *Example*: try the microbenchmark test (UPDATE)
    ```shell
    # on each compute node (CN)
    ./client_perf update 15 8 8
    ```

    Note that the microbenchmarks of Aceso can run simply without extra workload files, while the macrobenchmarks (*e.g.*, YCSB) need to be prepared first.

- Results.

    **Throughputs** (ops/s) will be displayed on each client terminal, example:
    ```shell
    total tpt: 562314
    ```

    **Latencies** (us) will be collected in files under the `./build/major-test/results` directory, such as `micro_update_lat.txt`, `micro_search_lat.txt`, where each line `i j` indicates that a latency of `i` µs appeared `j` times.

    Note that the latency files will be generated only when the parameter `coro_num_per_client` is set to `1`.

### On CNs - Prepare workloads

> **!!! Skip this step if you are using our created cluster with the provided account.**

1. Download all the testing workloads using sh `download-<xxx>-workload.sh` in directory `./setup` and unpack the workloads.
    ```shell
    cd ~/home/aceso/setup
    sudo bash download-ycsb-workload.sh
    sudo bash download-twitter-workload.sh
    ```

    Note that `ycsb-full` workloads contain 1 million keys, while `ycsb` workloads contain 0.1 million. In artifact evaluation, we recommend using `ycsb` workloads because they significantly reduce the initial loading time, without affecting Aceso's performance.

2. Execute the following command in directory `./setup` to split the workloads into `N` parts(`N` is the total number of client threads):
    ```shell
    python3 split-ycsb-workload.py [N]
    python3 split-twitter-workload.py [N]
    ```

3. Copy the split workloads to `./build/major-test/workloads`.
    ```shell
    cd ~/home/aceso
    mkdir -p ./build/major-test/workloads/
    cp ./setup/workloads/* ./build/major-test/workloads/
    cp ./setup/twi-workloads/* ./build/major-test/workloads/
    ```

    Then we can start testing Aceso using macrobenchmarks.

## Step-4 Reproduce all experiment results *(Results Reproduced)*

We provide code and scripts in `./exp` for reproducing our experiments. For more details, see [./exp/README.md](./exp).

## Acknowledgments

Some part of Aceso's codebase from [FUSEE](https://github.com/dmemsys/FUSEE), [SMART](https://github.com/dmemsys/SMART), and [Sherman](https://github.com/thustorage/Sherman).

The automatic testing scripts in `./exp` are based on the work done in [SMART](https://github.com/dmemsys/SMART) by Xuchuan. Special thanks to his contributions!