#!/bin/bash
# download YCSB A/B/C/D/UPDx workloads (1 million keys)
sudo apt install python3-pip -y
pip3 install gdown

# download workload
echo "downloading full-workloads.tgz"
if [ ! -d "./full-workloads.tgz" ]; then
  python3 ./download_gdrive.py 18DOv1pXXF64DnqW2qn9SWmstUwA34gDR full-workloads.tgz
fi

# decompress workload
echo "decompressing full-workloads files"
if [ ! -d "./workload-data" ]; then
  tar zxvf full-workloads.tgz
fi

