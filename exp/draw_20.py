import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'fig_20_2a.json').open(mode='r') as f:
    json_data = json.load(f)
with (Path(data_path) / f'fig_20_1a.json').open(mode='r') as f:
    json_data_tpt = json.load(f)
block_sizes = [16384, 65536, 262144, 1048576, 2097152, 4194304, 8388608, 16777216]
 
data = {
    "methods": ["Index", "Update"],
    "X_data": {
        "Index": block_sizes,
        "Update": block_sizes
    },
    "Y_data": {
        "Index": [(json_data['Y_data'][str(t)]['meta'] + json_data['Y_data'][str(t)]['hash']) for t in block_sizes],
        "Update": [json_data_tpt['Y_data'][str(t)]['update']  for t in block_sizes],
    },
}
print(data)
colors = {
    'Index': '#70BFFF',
    'Update': '#FF9900',
}
zorders = {
    'Index': 3,
    'Update': 2,
    'Block': 1
}
markers = {
    'Index': 'o',
    'Update': 's',
    'Block': '^'
}

plt.rc("font", size=6)
fig, ax1 = plt.subplots(figsize=(1.67, 1), dpi=300)
ax2 = ax1.twinx()
x = np.arange(len(block_sizes))

method = 'Index'
y_data = [y/1000000 for y in data["Y_data"][method]]
ax1.plot(
            x, y_data, label=method, linewidth=0.5, color=colors[method], zorder=zorders[method],
            marker=markers[method],
            markersize=3,
            markerfacecolor='none',
            markeredgewidth=0.5,
        )

method = 'Update'
y_data = [y for y in data["Y_data"][method]]
ax2.plot(
            x, y_data, label=method, linewidth=0.5, color=colors[method], zorder=zorders[method],
            marker=markers[method],
            markersize=3,
            markerfacecolor='none',
            markeredgewidth=0.5,
        )

ax1.tick_params(axis='y', pad=0)
ax1.tick_params(axis='x', pad=0)
ax1.spines['top'].set_linewidth(0.5)
ax1.spines['bottom'].set_linewidth(0.5)
ax1.spines['left'].set_linewidth(0.5)
ax1.spines['right'].set_linewidth(0.5)
ax1.spines['top'].set_visible(False)
ax1.tick_params(axis='y', which='both', length=2, width=0.5, pad=0)
ax1.tick_params(axis='both', which='both', length=2, width=0.5, pad=0)
ax1.tick_params(axis='x', which='both', length=2, width=0.5, pad=1)
ax1.set_ylabel('Recovery Time (s)', labelpad=0)

ax2.tick_params(axis='y', pad=0)
ax2.tick_params(axis='x', pad=0)
ax2.spines['top'].set_linewidth(0.5)
ax2.spines['bottom'].set_linewidth(0.5)
ax2.spines['left'].set_linewidth(0.5)
ax2.spines['right'].set_linewidth(0.5)
ax2.spines['top'].set_visible(False)
ax2.tick_params(axis='y', which='both', length=2, width=0.5, pad=0)
ax2.tick_params(axis='both', which='both', length=2, width=0.5, pad=0)
ax2.tick_params(axis='x', which='both', length=2, width=0.5, pad=1)
ax2.set_ylabel('Throughput (Mops/s)', labelpad=0, fontsize=6)
ax2.yaxis.set_label_coords(1.065, 0.44)

lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
legend = ax1.legend(lines + lines2, ['Index Recovery', 'Update Throughput'], loc='upper center', fontsize=5, ncol=1)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

x_labels = [r'$2^4$', r'$2^6$', r'$2^8$', r'$2^{10}$', r'$2^{11}$', r'$2^{12}$', r'$2^{13}$', r'$2^{14}$']
plt.xticks(x, x_labels)
xticks = ax1.get_xticklabels()

ax1.set_ylim(0, 2.25)
ax2.set_ylim(2.5, 4.75)

ax1.set_yticks([0.0, 0.5, 1.0, 1.5, 2.0])

for tick in xticks:
    tick.set_y(tick.get_position()[1] - 0.01)
ax1.set_xlabel("Block Size (KB)", labelpad=0)

plt.tight_layout(pad=0)
plt.savefig("./figures/fig_20.pdf", format='pdf')
plt.close()