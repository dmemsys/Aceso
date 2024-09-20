import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json
#from light to deep
blue_colors = ['#F8FAFF', '#9EC9E1', '#6BADD6', '#4291C7', '#084594']

data_path = "./results"
with (Path(data_path) / f'fig_14_2a.json').open(mode='r') as f:
    search_data = json.load(f)
data_path = "./results"
with (Path(data_path) / f'fig_14_1a.json').open(mode='r') as f:
    update_data = json.load(f)

X_data = ['Search', 'Update']
Y_data = {
    "Search": [search_data['Y_data']['aceso']['search'], 0],
    "Search-Degrade": [search_data['Y_data']['aceso']['search_degrade'], 0],
    "Update": [0, update_data['Y_data']['aceso']['update']],
    "Update-Reclaim": [0, update_data['Y_data']['aceso']['update_gc']],
}
colors = ['#70BFFF', '#FF9900']

x_list = np.arange(len(X_data))
plt.rc("font", size=6)
plt.rcParams['hatch.linewidth'] = 0.4

plt.tick_params(axis='both', which='both', length=2, width=0.5, pad=0.5)

fig, ax1 = plt.subplots(figsize=(1.67, 1), dpi=300)
bar_width = 0.4

ax1.set_ylabel('Search Throughput (Mops/s)', labelpad=1, fontsize=5)
ax1.yaxis.set_label_coords(-0.125, 0.46)
search_bars = ax1.bar(x_list-0.5*bar_width, Y_data['Search'], bar_width, label='Search', color=blue_colors[0],
            alpha=1,
            edgecolor='black', linewidth=0.5,
            hatch=''
       )
search_spe_bars = ax1.bar(x_list+0.5*bar_width, Y_data['Search-Degrade'], bar_width, label='Search-Degrade', color=blue_colors[2],
            alpha=1,
            edgecolor='black', linewidth=0.5,
            hatch='')
ax1.tick_params(axis='y', pad=1)
ax1.tick_params(axis='x', pad=1)
ax1.spines['top'].set_linewidth(0.5)
ax1.spines['bottom'].set_linewidth(0.5)
ax1.spines['left'].set_linewidth(0.5)
ax1.spines['right'].set_linewidth(0.5)
ax1.spines['top'].set_visible(False)
ax1.tick_params(axis='y', which='both', length=2, width=0.5, pad=1)
ax1.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

ax2 = ax1.twinx()
ax2.set_ylabel('Update Throughput (Mops/s)', labelpad=1, fontsize=5)
ax2.yaxis.set_label_coords(1.0875, 0.45)
update_bars = ax2.bar(x_list-0.5*bar_width, Y_data['Update'], bar_width, label='Update', color=blue_colors[0],
            alpha=1,
            edgecolor='black', linewidth=0.5,
            hatch=''
       )
update_spe_bars = ax2.bar(x_list+0.5*bar_width, Y_data['Update-Reclaim'], bar_width, label='Update-Reclaim', color=blue_colors[2],
            alpha=1,
            edgecolor='black', linewidth=0.5,
            hatch='')
ax2.tick_params(axis='y', pad=1)
ax2.tick_params(axis='x', pad=1)
ax2.spines['top'].set_linewidth(0.5)
ax2.spines['bottom'].set_linewidth(0.5)
ax2.spines['left'].set_linewidth(0.5)
ax2.spines['right'].set_linewidth(0.5)
ax2.spines['top'].set_visible(False)
ax2.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
legend = ax1.legend(lines + lines2, ['Normal', 'Special'], loc='upper right', fontsize=6, ncol=1)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

ax1.set_ylim(0, 26)
ax2.set_ylim(0, 8)

plt.xticks(x_list, X_data)

annotate_flag = True
for fusees, acesos in zip(zip(search_bars, update_bars), zip(search_spe_bars, update_spe_bars)):
    for bar_fus, bar_ace in zip(fusees, acesos):

        height_p50 = bar_fus.get_height()
        height_p99 = bar_ace.get_height()
        print(height_p50, height_p99)
        if height_p50 > 0.1:
            if annotate_flag:
                ax1.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                                xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)
                annotate_flag = False
            else:
                ax2.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                                xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)

plt.tight_layout(pad=0.1)
plt.savefig("./figures/fig_14.pdf", format='pdf')
plt.close()
