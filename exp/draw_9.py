import matplotlib.pyplot as plt
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
with (Path(data_path) / f'fig_9a.json').open(mode='r') as f:
        json_data = json.load(f)

op_names = [
    "aceso_insert_p50",
    "aceso_insert_p99",
    "aceso_update_p50",
    "aceso_update_p99",
    "aceso_search_p50",
    "aceso_search_p99",
    "aceso_delete_p50",
    "aceso_delete_p99",
    "fusee_insert_p50",
    "fusee_insert_p99",
    "fusee_update_p50",
    "fusee_update_p99",
    "fusee_search_p50",
    "fusee_search_p99",
    "fusee_delete_p50",
    "fusee_delete_p99"
]
data = {
    "methods": ["aceso", "fusee"],
    "bar_groups": ["insert", "update", "search", "delete"],
    "Y_data": {
        op : json_data["Y_data"][op]
        for op in op_names
    }
}

methods = data["methods"]
bar_groups = data["bar_groups"]
aceso_p50 = [data["Y_data"][f"aceso_{op}_p50"][0] for op in bar_groups]
aceso_p99 = [data["Y_data"][f"aceso_{op}_p99"][0] for op in bar_groups]
fusee_p50 = [data["Y_data"][f"fusee_{op}_p50"][0] for op in bar_groups]
fusee_p99 = [data["Y_data"][f"fusee_{op}_p99"][0] for op in bar_groups]
labels = ["INSERT", "UPDATE", "SEARCH", "DELETE"]

bar_width = 0.4
r1 = np.arange(len(bar_groups))
r2 = [x + bar_width for x in r1]

plt.rc("font", size=6)

fig = plt.figure(figsize=(1.67, 0.9), dpi=300)

bars_fusee_p50 = plt.bar(r1, fusee_p50, color=blue_colors[0], width=bar_width, edgecolor='black', label='FUS-P50', zorder=1, linewidth=0.5)
bars_aceso_p50 = plt.bar(r2, aceso_p50, color=blue_colors[2], width=bar_width, edgecolor='black', label='ACE-P50', zorder=1, linewidth=0.5)
bars_fusee_p99 = plt.bar(r1, fusee_p99, color=blue_colors[0], width=bar_width, edgecolor='grey', label='FUS-P99', alpha=0.5, zorder=0, linewidth=0.5)
bars_aceso_p99 = plt.bar(r2, aceso_p99, color=blue_colors[2], width=bar_width, edgecolor='grey', label='ACE-P99', alpha=0.5, zorder=0, linewidth=0.5)

# Add annotations
for fusees, acesos in zip(zip(bars_fusee_p50, bars_fusee_p99), zip(bars_aceso_p50, bars_aceso_p99)):
    for bar_fus, bar_ace in zip(fusees, acesos):
        height_p50 = bar_fus.get_height()
        height_p99 = bar_ace.get_height()
        plt.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                        xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)

method_labels = ["Insert", "Update", "Search", "Delete"]
plt.xticks([r + bar_width / 2 for r in range(len(bar_groups))], method_labels)

legend = plt.legend(fontsize=5, ncol=2,  loc='upper right')
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.ylim(0,175)
plt.gca().spines['top'].set_linewidth(0.5)
plt.gca().spines['bottom'].set_linewidth(0.5)
plt.gca().spines['left'].set_linewidth(0.5)
plt.gca().spines['right'].set_linewidth(0.5)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.ylabel("Latency (us)", labelpad=1)
plt.tick_params(axis='both', which='both', length=2, width=0.5, pad=1)

plt.tight_layout(pad=0)
plt.savefig("./figures/fig_9.pdf", format='pdf')
plt.close()
