#!/bin/bash
# download YCSB A/B/C/D/UPDx workloads (0.1 million keys)
sudo apt install python3-pip -y
pip3 install gdown

echo "downloading YCSB workloads"
if [ ! -d "./workloads.tgz" ]; then
  python3 ./download_gdrive.py 1D7RWjTDZjFtK88qO0TJOxePmsPPnehrR workloads.tgz
fi

echo "decompressing YCSB workloads"
if [ ! -d "./workloads" ]; then
  tar zxvf workloads.tgz
fi