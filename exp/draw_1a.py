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
with (Path(data_path) / f'fig_1a.json').open(mode='r') as f:
    json_data = json.load(f)

methods = ["insert", "update", "search", "delete"]
fusee_micro_data = [[], [], []]
for method in methods:
    fusee_micro_data[0].append(json_data['Y_data']['3'][method])
    fusee_micro_data[1].append(json_data['Y_data']['2'][method])
    fusee_micro_data[2].append(json_data['Y_data']['1'][method])
fusee_avg_cas_data = [[], [], []]
for method in methods:
    fusee_avg_cas_data[0].append(json_data['CAS_data']['3'][method] / json_data['Y_data']['3'][method])
    fusee_avg_cas_data[1].append(json_data['CAS_data']['2'][method] / json_data['Y_data']['2'][method])
    fusee_avg_cas_data[2].append(json_data['CAS_data']['1'][method] / json_data['Y_data']['1'][method])

mpl.rcParams['font.size'] = 6

methods = ["insert", "update", "search", "delete"]
labels = ["3-Rep", "2-Rep", "1-Rep"]
colors = list(my_main_colors.values())
markers = ["o", "s", "^"]
zorders = [3,2,1]
hatches = ['/', '\\\\', 'xx']
plt.rcParams['hatch.linewidth'] = 0.4
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(2.14, 1), dpi=300)
fig.subplots_adjust(hspace=1)

for i in range(3):
    ax1.bar(
        np.arange(len(methods)) + (i-1) * 0.3,
        fusee_micro_data[i],
        width=0.3,
        color=blue_colors[i*2],
        label=labels[i],
        alpha=1,
        edgecolor='black', linewidth=0.5,
        # hatch=hatches[i]
    )
    ax2.bar(
        np.arange(len(methods)) + (i-1) * 0.3,
        fusee_micro_data[i],
        width=0.3,
        color=blue_colors[i*2],
        alpha=1,
        edgecolor='black', linewidth=0.5,
        # hatch=hatches[i]
    )

ax1_twin = ax1.twinx()
ax2_twin = ax2.twinx()
cas_label = 'Avg CAS'
for i, method in enumerate(methods):
    if method == 'search':
        continue
    ax1_twin.plot(
        [i-0.3, i, i+0.3],
        [fusee_avg_cas_data[0][i], fusee_avg_cas_data[1][i], fusee_avg_cas_data[2][i]],
        label=cas_label, linewidth=0.5, color='black', 
        marker='^',
        markersize=4,
        markerfacecolor='white',
        linestyle = '--',
        markeredgewidth=0.5,
    )
    ax2_twin.plot(
        [i-0.3, i, i+0.3],
        [fusee_avg_cas_data[0][i], fusee_avg_cas_data[1][i], fusee_avg_cas_data[2][i]],
        label=cas_label, linewidth=0.5, color='black', 
        marker='^',
        markersize=4,
        markerfacecolor='white',
        linestyle = '--',
        markeredgewidth=0.5,
    )
ax1_twin.set_yticks([])
ax2_twin.set_yticks([ 0, 2, 4])
import matplotlib.ticker as ticker
ax2_twin.yaxis.set_minor_locator(ticker.FixedLocator([1, 3]))
ax1_twin.set_ylim(18, 23)
ax2_twin.set_ylim(0, 4.5)
ax1_twin.tick_params(axis='y', pad=1)
ax1_twin.tick_params(axis='x', pad=1)
ax1_twin.spines['top'].set_linewidth(0.5)
ax1_twin.spines['bottom'].set_linewidth(0.5)
ax1_twin.spines['left'].set_linewidth(0.5)
ax1_twin.spines['right'].set_linewidth(0.5)
ax1_twin.spines['top'].set_visible(False)
ax1_twin.spines['bottom'].set_visible(False)
ax1_twin.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

ax2_twin.tick_params(axis='y', pad=1)
ax2_twin.tick_params(axis='x', pad=1)
ax2_twin.spines['top'].set_linewidth(0.5)
ax2_twin.spines['bottom'].set_linewidth(0.5)
ax2_twin.spines['left'].set_linewidth(0.5)
ax2_twin.spines['right'].set_linewidth(0.5)
ax2_twin.spines['top'].set_visible(False)
ax2_twin.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)
ax2_twin.set_ylabel("Avg CAS (#)")
ax2_twin.yaxis.set_label_coords(1.06, 1)

ax1.set_ylim(19, 24.5)
ax2.set_ylim(0, 5.5)

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

ax1.set_yticks([20, 22, 24])
ax2.set_yticks([0, 2, 4])
method_labels = ["Insert", "Update", "Search", "Delete"]
ax2.set_xticks(np.arange(len(method_labels)))
ax2.set_xticklabels(method_labels)
ax2.set_xlabel("Request Type", labelpad=0)
ax2.set_ylabel("Throughput (Mops/s)")
ax2.yaxis.set_label_coords(-0.083, 0.95)

lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax1_twin.get_legend_handles_labels()
legend = ax1_twin.legend(lines + lines2, labels+['Avg CAS'], loc='upper left', fontsize=6, ncol=2, bbox_to_anchor=(0., 1.02, 1., .102))
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad=0.2, h_pad=0)
plt.savefig("./figures/fig_1a.pdf", format='pdf')
plt.close()