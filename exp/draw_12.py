import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json
from mpl_toolkits.axes_grid1.inset_locator import mark_inset
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
#from light to deep
blue_colors = ['#F8FAFF', '#9EC9E1', '#6BADD6', '#4291C7', '#084594']

my_colors2 = {
    'sky' : '#B3D9FF',
    'blue' : '#0066CC',
    'yellow' : '#FFC000',
    'milk': '#FAEADC',
    'skin': '#FFA09E'
}

data_path = "./results"
with (Path(data_path) / f'fig_12a.json').open(mode='r') as f:
    json_data = json.load(f)

mem_pool_size = 48 * 1024 * 1024 * 1024 * 5

methods = ["FUSEE", "Aceso"]
X_data = ["FUSEE", "Aceso"]
# PS: json_data["Y_data"]["aceso"][0] * 2 / 3 is from the erasure coding's setting in our experiments "data : parity = 3 : 2" 
Y_data = {
    "Aceso": [json_data["Y_data"]["aceso"][0], json_data["Y_data"]["aceso"][1] - json_data["Y_data"]["aceso"][0], json_data["Y_data"]["aceso"][0] * 2 / 3],
    "FUSEE": [json_data["Y_data"]["fusee"][0], json_data["Y_data"]["fusee"][1] - json_data["Y_data"]["fusee"][0], mem_pool_size - json_data["Y_data"]["fusee"][1]]
}

labels = [['Valid', 'Redundancy', 'Free'], ['Valid', 'Delta', 'Redundancy', 'Free']]
colors = {
    'Valid' : ['#084594'],
    'Redundancy' : ['#6BADD6'],
    'Delta' : ['#FFC000'],
    'Free' : ['#DCEAF7']
}
plt.rc("font", size=6)



fig, ax = plt.subplots(figsize=(3.34, 0.7), dpi=300)

bar_width = 0.6
index = range(len(methods))

legend_handles = []
legend_labels = []

bottom = [0, 0, 0]
for i, method in enumerate(methods):
    delta = Y_data[method][0]/(1024*1024*1024)
    total = 0
    print(colors[labels[i][0]])
    ax_bar = ax.barh(method, delta, color=colors[labels[i][0]], label=labels[i][0], left=0, height=bar_width)
    if i == 1:
        legend_handles.append(ax_bar[0])
        legend_labels.append(labels[i][0])
    
    total += delta
    delta = Y_data[method][1]/(1024*1024*1024)
    ax_bar = ax.barh(method, delta, color=colors[labels[i][1]], label=labels[i][1], left=total, height=bar_width)
    if i == 1:
        legend_handles.append(ax_bar[0])
        legend_labels.append(labels[i][1])
    
    total += delta
    delta = Y_data[method][2]/(1024*1024*1024)
    ax_bar = ax.barh(method, delta, color=colors[labels[i][2]], label=labels[i][2], left=total, height=bar_width)
    if i == 1:
        legend_handles.append(ax_bar[0])
        legend_labels.append(labels[i][2])
        print(total+delta, (json_data["Y_data"]["fusee"][1]/(1024*1024*1024)))
        plt.annotate(f'{(total+delta) / (json_data["Y_data"]["fusee"][1]/(1024*1024*1024)):.2f}X', xy=(ax_bar[0].get_x() + ax_bar[0].get_width()+8.5, 1-0.25), 
                        xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)
    
    if i == 1:
        total += delta
        delta = (mem_pool_size/(1024*1024*1024)) - total
        ax_bar = ax.barh(method, delta, color=colors[labels[i][3]], label=labels[i][3], left=total, height=bar_width)
        legend_handles.append(ax_bar[0])
        legend_labels.append(labels[i][3])

axins = inset_axes(ax, width="25%", height="15%",loc='lower left',
                   bbox_to_anchor=(0.03, 0.74, 1, 1),
                   bbox_transform=ax.transAxes)

bottom = [0, 0, 0]
for i, method in enumerate(methods):
    delta = Y_data[method][0]/(1024*1024*1024)
    total = 0
    axins.barh(method, delta, color=colors[labels[i][0]], label=labels[i][0], left=0, height=bar_width)
    total += delta
    delta = Y_data[method][1]/(1024*1024*1024)
    axins.barh(method, delta, color=colors[labels[i][1]], label=labels[i][1], left=total, height=bar_width)
    total += delta
    delta = Y_data[method][2]/(1024*1024*1024)
    axins.barh(method, delta, color=colors[labels[i][2]], label=labels[i][2], left=total, height=bar_width)
    if i == 1:
        total += delta
        delta = (mem_pool_size/(1024*1024*1024)) - total
        axins.barh(method, delta, color=colors[labels[i][3]], label=labels[i][3], left=total, height=bar_width)

left_x = json_data["Y_data"]["aceso"][0] / 1024 / 1024 / 1024 - 0.25
right_x = json_data["Y_data"]["aceso"][1] / 1024 / 1024 / 1024 + 0.25
axins.set_xlim(left_x, right_x)
axins.set_ylim(1-0.01, 1+0.01)
ax.set_ylim(-0.5, 2.5)

ax.spines['bottom'].set_linewidth(0.5)
ax.spines['left'].set_linewidth(0.5)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

ax.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

axins.spines['bottom'].set_linewidth(0.5)
axins.spines['left'].set_linewidth(0.5)
axins.spines['top'].set_linewidth(0.5)
axins.spines['right'].set_linewidth(0.5)
axins.tick_params(axis='x', which='both', length=2, width=0.5, pad=0.5, labelsize=5)
axins.tick_params(axis='y', which='both', length=0, width=0.5, pad=0.5, labelsize=5)
axins.set_yticklabels([])
mark_inset(ax, axins, loc1=3, loc2=4, fc="none", ec='k', lw=0.3, ls='--')

legend = ax.legend(legend_handles, legend_labels,fontsize=6, ncol=4, loc='upper right', columnspacing = 0.5, handletextpad = 0.2, bbox_to_anchor=(0., 1.05, 1., 0.01))
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

ax.set_xlabel('Size (GB)', labelpad=0)


plt.tight_layout(pad=0.2, h_pad=0)
plt.savefig("./figures/fig_12.pdf", format='pdf')
plt.close()