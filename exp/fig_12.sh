#!/bin/bash

start_time=$(date +%s)

python3 fig_12.py
python3 draw_12.py

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"