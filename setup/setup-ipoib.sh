#!/bin/bash

if [ -f "/etc/netplan/50-cloud-init.yaml" ]; then
    sudo rm /etc/netplan/50-cloud-init.yaml
fi

# find the line that contains "10.10.1.x"
line=$(ifconfig | grep -E 'inet\s10\.10\.1\.[0-9]{1,2}\b')

# extract the ip address "10.10.1.x"
ip=$(echo "$line" | grep -oP '10\.10\.1\.[0-9]{1,2}' | head -n 1)

# get new ip address for RDMA "10.10.10.x"
new_ip=$(echo "$ip" | sed 's/10\.10\.1/10.10.10/')

# set new ip address for RDMA(ib0)
content="network:
    ethernets:
        ib0:
            addresses: [$new_ip/24]
    version: 2"

echo "$content" | sudo tee -a /etc/netplan/50-cloud-init.yaml > /dev/null

echo "$line" | grep -oEm 1 '10\.10\.1\.[0-9]{1,2}'
