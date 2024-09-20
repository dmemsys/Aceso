import matplotlib.pyplot as plt
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'fig_16a.json').open(mode='r') as f:
    json_data = json.load(f)

times = [
        2,
        6,
        10,
        14,
        18,
        22,
        26,
        30
    ] 
data = {
    "methods": ["Meta", "Index", "Block"],
    "X_data": {
        "Meta": json_data['X_data'],
        "Index": json_data['X_data'],
        "Block": json_data['X_data'],
    },
    "Y_data": {
        "Meta":  [json_data['Y_data'][f"{t}"]['time_dict']['meta'] for t in times],
        "Index": [json_data['Y_data'][f"{t}"]['time_dict']['hash'] for t in times],
        "Block": [json_data['Y_data'][f"{t}"]['time_dict']['data'] for t in times],
    },
}
X_list_data_count = [(json_data['Y_data'][f"{t}"]['time_dict']['index-lblock-count'] + json_data['Y_data'][f"{t}"]['time_dict']['data-block-count']) for t in times]
x_list_data_size_GB = [f"{(x * 2 / 1024):.1f}" for x in X_list_data_count]
colors = {
    'Meta': '#70BFFF',
    'Index': '#FF9900',
    'Block': '#0066CC'
}
zorders = {
    'Meta': 3,
    'Index': 2,
    'Block': 1
}
markers = {
    'Meta': 'o',
    'Index': 's',
    'Block': '^'
}

fig= plt.figure(figsize=(1.67, 1), dpi=300)
plt.rc("font", size=6)

for method in data["methods"]:
    x = data["X_data"][method]
    y_data = [y / 1000000 for y in data["Y_data"][method]]
    plt.plot(
                x, y_data, label=method, linewidth=0.5, color=colors[method], zorder=zorders[method],
                marker=markers[method],
                markersize=3,
                markerfacecolor='none',
                # markeredgecolor='black',
                markeredgewidth=0.5,
            )

plt.tick_params(axis='both', which='both', length=2, width=0.5)

plt.gca().spines['top'].set_linewidth(0.5)
plt.gca().spines['bottom'].set_linewidth(0.5)
plt.gca().spines['left'].set_linewidth(0.5)
plt.gca().spines['right'].set_linewidth(0.5)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

plt.gca().tick_params(axis='x', pad=1)
plt.gca().tick_params(axis='y', pad=1)
plt.xticks(data["X_data"]['Meta'], x_list_data_size_GB, fontsize=4)
plt.xlabel("Lost Data Size (GB)", labelpad=0.5)
plt.ylabel("Time (s)", labelpad=0)
plt.yticks([0, 3, 6, 9, 12])

legend = plt.legend(fontsize=7)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad=0.15, h_pad=0)
plt.savefig("./figures/fig_16.pdf", format='pdf')
plt.close()