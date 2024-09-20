import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'fig_19a.json').open(mode='r') as f:
    json_data = json.load(f)
blue_colors = ['#F8FAFF', '#9EC9E1', '#6BADD6', '#4291C7', '#084594']
intervals = json_data["X_data"]
methods = ["size", "copy-xor", "compress", "decompress", "xor"]
method_labels = ['Size', 'Copy&XOR', 'Compress', 'Decompress', 'XOR']
data = {
    "methods": methods,
    "X_data": {x : intervals for x in methods},
    "Y_data": {x : [json_data["Y_data"][f"{t}"][x] for t in intervals] for x in methods},
}

my_main_colors = {
    'blue': '#C8C8FF',
    'green': '#A0E6B4',
    'grey': '#ACDCD7',
    'milk': '#FFD7AF',
    'red': '#FCB0A6',
}
my_colors = {
    'sky' : '#70BFFF',
    'orange' : '#FF9900',
    'blue' : '#0066CC',
    'grey' : '#808080',
    'red' : '#A0E6B4'
}
colors = list(my_colors.values())


markers = ['o','s','^','x', '+']

x_list = np.arange(len(intervals))
plt.rc("font", size=6)
plt.rcParams['hatch.linewidth'] = 0.4

plt.tick_params(axis='both', which='both', length=2, width=0.5, pad=0.5)

fig, ax1 = plt.subplots(figsize=(1.67, 1.2), dpi=300)

method = "size"
y_data = [y / (1024 * 1024) for y in data["Y_data"][method]]
bars = ax1.bar(
                x_list, y_data, label=method, width=0.4,  color=blue_colors[0],
                alpha=1,
                edgecolor='black', linewidth=0.5,
                hatch=''
            )

for i, bar_fus in enumerate(bars):
    height_p50 = bar_fus.get_height()

ax1.tick_params(axis='y', pad=1)
ax1.tick_params(axis='x', pad=1)

ax1.spines['bottom'].set_linewidth(0.5)
ax1.spines['left'].set_linewidth(0.5)
ax1.spines['right'].set_visible(False)
ax1.spines['top'].set_visible(False)
ax1.tick_params(axis='y', which='both', length=2, width=0.5, pad=1)
ax1.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)
ax2 = ax1.twinx()

ax2.set_ylabel('Time (s)', labelpad=1)
ax2.yaxis.set_label_coords(1.15, 0.46)

t_methods = ["copy-xor", "compress", "decompress", "xor"]
for i, method in enumerate(t_methods):
    if method == "size":
        continue
    y_data = [y / 1000000 for y in data["Y_data"][method]]
    ax2.plot(
                x_list, y_data, label=method, linewidth=0.5, color=colors[i], 
                marker=markers[i],
                markersize=3,
                markerfacecolor='none',
                markeredgewidth=0.5,
            )

ax2.tick_params(axis='y', pad=1)
ax2.tick_params(axis='x', pad=1)
ax2.spines['top'].set_linewidth(0.5)
ax2.spines['bottom'].set_linewidth(0.5)
ax2.spines['left'].set_linewidth(0.5)
ax2.spines['right'].set_linewidth(0.5)
ax2.spines['top'].set_visible(False)
ax2.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

lines, labels = ax1.get_legend_handles_labels()
alines, alabels = ax2.get_legend_handles_labels()
method_labels = ['Size', 'Copy&XOR', 'Compress', 'Decompress', 'XOR']
legend = ax1.legend(lines+alines, method_labels, loc='upper left', fontsize=6, ncol=1)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

ax1.set_ylim(0, 31)
ax2.set_ylim(0, 1.1)

ax2.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])

x_labels = ['256', '512', '1024', '2048']
plt.xticks(x_list, x_labels)
ax1.set_ylabel('Compressed Size (MB)', labelpad=0)
ax1.yaxis.set_label_coords(-0.11, 0.46)
ax1.set_xlabel("Index Size (MB)", labelpad=0)

plt.tight_layout(pad=0, h_pad=0)
plt.savefig("./figures/fig_19.pdf", format='pdf')
plt.close()
