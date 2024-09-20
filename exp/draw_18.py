import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'fig_18a.json').open(mode='r') as f:
    json_data = json.load(f)

intervals = json_data["X_data"]
 
data = {
    "methods": ["Meta", "Index", "Block"],
    "X_data": {
        "Meta": intervals,
        "Index": intervals,
        "Block": intervals,
    },
    "Y_data": {
        "Meta":  [json_data['Y_data'][f"{t}"]['meta'] for t in intervals],
        "Index": [json_data['Y_data'][f"{t}"]['hash'] for t in intervals],
        "Block": [json_data['Y_data'][f"{t}"]['data'] for t in intervals],
    },
}

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
x = np.arange(len(intervals))
for method in data["methods"]:
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

plt.xticks([0, 4, 9, 14], ['100', '500', '1000', '1500'], fontsize=6)

plt.xlabel("Interval (ms)", labelpad=1)
plt.ylabel("Time (s)", labelpad=0)

legend = plt.legend(fontsize=6)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad=0)
plt.savefig("./figures/fig_18.pdf", format='pdf')
plt.close()