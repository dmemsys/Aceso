import matplotlib.pyplot as plt
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'fig_15a.json').open(mode='r') as f:
    json_data = json.load(f)
        
data = {
    "methods": ["aceso", "fusee"],
    "X_data": {
        "aceso": json_data['X_data']['aceso'],
        "fusee": json_data['X_data']['fusee'],
    },
    "Y_data": {
        
        "aceso": json_data['Y_data']['aceso'],
        "fusee": json_data['Y_data']['fusee'],
    },
}
colors = {
    'aceso': '#70BFFF',
    'fusee': '#FF9900',
    'clover': '#0066CC'
}
zorders = {
    'aceso': 3,
    'fusee': 2,
    'clover': 1
}
markers = {
    'aceso': 'o',
    'fusee': 's',
    'clover': '^'
}
translate = {
    'aceso': 'Aceso',
    'fusee': 'FUSEE',
    'clover': 'Clover'
}

fig= plt.figure(figsize=(1.67, 1), dpi=300)

plt.rc("font", size=6)

for method in data["methods"]:
    x = data["X_data"][method]
    y_data = [y / 1000000 for y in data["Y_data"][method]]
    plt.plot(
                x, y_data, label=translate[method], linewidth=0.5, color=colors[method], zorder=zorders[method],
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
plt.xlabel("Update Ratio (%)", labelpad=0)
plt.ylabel("Throughput (Mops/s)", labelpad=0)
ax = plt.gca()
ax.yaxis.set_label_coords(-0.09,0.47)
plt.yticks([0, 5, 10, 15, 20,])

plt.gca().tick_params(axis='x', pad=0)
plt.gca().tick_params(axis='y', pad=0)

legend = plt.legend(fontsize=8)
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)


plt.tight_layout(pad=0.2)
plt.savefig("./figures/fig_15.pdf", format='pdf')
plt.close()