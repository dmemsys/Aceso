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
with (Path(data_path) / f'fig_8a.json').open(mode='r') as f:
    rep3_data = json.load(f)

methods = ["insert", "update", "search", "delete"]
fusee_micro_data = [[], []]
for method in methods:
    fusee_micro_data[0].append(rep3_data['Y_data']['fusee'][method])
    fusee_micro_data[1].append(rep3_data['Y_data']['aceso'][method])


mpl.rcParams['font.size'] = 6

methods = ["insert", "update", "search", "delete"]
labels = ["FUSEE", "Aceso"]
colors = list(my_main_colors.values())
markers = ["o", "s", "^"]
zorders = [3,2,1]
hatches = ['/', '\\\\', 'xx']
plt.rcParams['hatch.linewidth'] = 0.4
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(1.67, 0.9), dpi=300)
fig.subplots_adjust(hspace=0.5)
fusee_up_bars = []
fusee_lo_bars = []
aceso_up_bars = []
aceso_lo_bars = []

for i in range(2):
    up_bars = ax1.bar(
        np.arange(len(methods)) + (i-0.5) * 0.4,
        fusee_micro_data[i],
        width=0.4,
        color=blue_colors[i*2],
        label=labels[i],
        alpha=1,
        edgecolor='black', linewidth=0.5,
    )
    lo_bars = ax2.bar(
        np.arange(len(methods)) + (i-0.5) * 0.4,
        fusee_micro_data[i],
        width=0.4,
        color=blue_colors[i*2],
        alpha=1,
        edgecolor='black', linewidth=0.5,
    )
    if i == 0:
        fusee_lo_bars = lo_bars
        fusee_up_bars = up_bars
    else:
        aceso_lo_bars = lo_bars
        aceso_up_bars = up_bars

# Add annotations
for fusees, acesos in zip(zip(fusee_lo_bars, fusee_up_bars), zip(aceso_lo_bars, aceso_up_bars)):
    for bar_fus, bar_ace in zip(fusees, acesos):
        height_p50 = bar_fus.get_height()
        height_p99 = bar_ace.get_height()
        ax2.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                        xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)
        ax1.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                        xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)

ax1.set_ylim(19, 26)
ax2.set_ylim(0, 7)

ax1.spines['bottom'].set_visible(False)
ax1.spines['top'].set_visible(False)
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

ax1.set_yticks([20, 25])
ax2.set_yticks([0, 5])

ax2.set_xticks(np.arange(len(methods)))
method_labels = ["Insert", "Update", "Search", "Delete"]
ax2.set_xticklabels(method_labels)
ax2.set_ylabel("Throughput (Mops/s)")
ax2.yaxis.set_label_coords(-0.1, 0.96)

legend = ax1.legend(loc="upper left")
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad=0.2, h_pad=0)
plt.savefig("./figures/fig_8.pdf", format='pdf')
plt.close()