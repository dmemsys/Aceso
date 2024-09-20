#!/bin/bash
sudo killall -9 memcached
ifconfig_output=$(ifconfig)

if [[ $ifconfig_output =~ "10.10.10.1" ]]; then
    memcached -u root -I 128m -m 4096 -c 1024 -l 10.10.10.1 &
    disown
    echo "memcached has been started"
else
    echo "memcached pass"
fi