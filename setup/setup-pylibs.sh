#!/bin/bash

# paramiko
sudo apt-get -y --force-yes install python3-pip
pip3 install --upgrade pip
pip3 install paramiko==3.2.0

# func_timeout
pip3 install func_timeout

# matplotlib
pip3 install matplotlib

pip3 install psutil