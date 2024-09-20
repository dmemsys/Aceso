import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from pathlib import Path
import json

#from light to deep
blue_colors = ['#F8FAFF', '#9EC9E1', '#6BADD6', '#4291C7', '#084594']

my_colors = {
    'orange' : '#FF9900',
    'yellow' : '#FFC000',
    'blue' : '#0066CC',
    'sky' : '#70BFFF'
}
my_colors2 = {
    'sky' : '#B3D9FF',
    'blue' : '#0066CC',
    'yellow' : '#FFC000',
    'milk': '#FAEADC',
    'skin': '#FFA09E'
}

my_main_colors = {
    'blue': '#C8C8FF',
    'green': '#A0E6B4',
    'grey': '#ACDCD7',
    'milk': '#FFD7AF',
    'red': '#FCB0A6',
}

data_path = "./results"
with (Path(data_path) / f'fig_1b.json').open(mode='r') as f:
    json_data = json.load(f)
index_sizes = ['64', '128', '256', '512']

markers = {
    'insert': 'o',
    'update': 's',
    'search': '^',
    'delete': 'x',
}
colors = {
    'insert': '#70BFFF',
    'update': '#FF9900',
    'search': '#0066CC',
    'delete': '#808080'
}

mpl.rcParams['font.size'] = 6

methods = ["insert", "update", "search", "delete"]
plt.rcParams['hatch.linewidth'] = 0.4
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(1.36, 1), dpi=300)
fig.subplots_adjust(hspace=1.5)
fusee_up_bars = []
fusee_lo_bars = []
aceso_up_bars = []
aceso_lo_bars = []
x = np.arange(len(index_sizes))

method_labels = ["IN", "UP", "SE", "DE"]
for i, method in enumerate(methods):
    y = [json_data['Y_data'][index_size][method] for index_size in index_sizes]
    up_bars = ax1.plot(
                x, y, label=method_labels[i], linewidth=0.5, color=colors[method],
                marker=markers[method],
                markersize=3,
                markerfacecolor='none',
                markeredgewidth=0.5,
            )
    lo_bars = ax2.plot(
                x, y, label=method_labels[i], linewidth=0.5, color=colors[method],
                marker=markers[method],
                markersize=3,
                markerfacecolor='none',
                markeredgewidth=0.5,
            )

ax1.set_ylim(14, 36)
ax2.set_ylim(0, 7)
ax1.set_xlim(-0.4, 3.4)
ax2.set_xlim(-0.4, 3.4)

ax1.spines['bottom'].set_visible(False)
ax2.spines['top'].set_visible(False)

ax1.tick_params(axis='x', which='both', length=0)
ax2.xaxis.tick_bottom()

d = .01
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False, linewidth=0.5)
ax1.plot((-d, +d), (-d, +d), **kwargs)
kwargs.update(transform=ax2.transAxes)
ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)

ax1.tick_params(axis='y', which='both', length=2, width=0.5, pad=1)
ax2.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

ax1.spines['bottom'].set_linewidth(0.5)
ax1.spines['left'].set_linewidth(0.5)
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)


ax2.spines['bottom'].set_linewidth(0.5)
ax2.spines['left'].set_linewidth(0.5)
ax2.spines['top'].set_visible(False)
ax2.spines['right'].set_visible(False)

ax1.set_yticks([15, 20, 25, 30, 35])
ax1.set_yticklabels(['15', '', '25', '', '35'])
ax2.set_yticks([0, 3, 6])
ax2.set_xticks(x)

x_ticklabels = ['64', '128', '256', '512']
xxxx = [r'$2^6$', r'$2^7$', r'$2^8$', r'$2^9$']
ax2.set_xticklabels(x_ticklabels)
ax2.set_ylabel("Throughput (Mops/s)", labelpad=1)
ax2.set_xlabel("Transfer Size (MB)", labelpad=0)
ax2.yaxis.set_label_coords(-0.13, 1)

legend = ax1.legend(ncol=2, loc="upper center", fontsize=5, columnspacing=0.5, bbox_to_anchor=(0.5, 1.1)) 
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad = 0.1, h_pad=0.2)
plt.savefig("./figures/fig_1b.pdf", format='pdf')
plt.close()