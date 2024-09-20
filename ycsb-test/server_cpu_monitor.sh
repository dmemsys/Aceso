#!/bin/bash

python_script="server_cpu_monitor.py"

nohup python3 "$python_script" &
disown

echo "Python script started in the background with PID $!"