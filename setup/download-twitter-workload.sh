#!/bin/bash
# download Twitter workloads
sudo apt install python3-pip -y
pip3 install gdown

echo "downloading twi-workloads.tgz"
if [ ! -d "./twi-workloads.tgz" ]; then
  python3 ./download_gdrive.py 16gRhG6Qyaw9z-VSptO32tLWieK1j_Gu2 twi-workloads.tgz
fi

echo "decompressing twi-workload files"
if [ ! -d "./twi-workloads" ]; then
  mkdir -p "./twi-workloads"
  tar zxvf twi-workloads.tgz -C ./twi-workloads
fi