#!/bin/bash

start_time=$(date +%s)

for EXP_NUM in fig_1a fig_1b fig_8 fig_9 fig_10 fig_11 fig_12 fig_13 fig_14 fig_15 fig_16 fig_17 fig_18 fig_19 fig_20 tab_2; do
  sh ${EXP_NUM}.sh
  sleep 10
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Total used time: $(($cost_time/60)) min $(($cost_time%60)) s"
