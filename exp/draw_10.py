import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json
#from light to deep
blue_colors = ['#F8FAFF', '#9EC9E1', '#6BADD6', '#4291C7', '#084594']

data_path = "./results"
with (Path(data_path) / f'fig_10a.json').open(mode='r') as f:
        json_data_a = json.load(f)
with (Path(data_path) / f'fig_10b.json').open(mode='r') as f:
        json_data_b = json.load(f)
with (Path(data_path) / f'fig_10c.json').open(mode='r') as f:
        json_data_c = json.load(f)
with (Path(data_path) / f'fig_10d.json').open(mode='r') as f:
        json_data_d = json.load(f)
data_sets = [
    {
        "methods": ["fusee", "aceso"],
        "X_data": {
            "aceso": [json_data_a['X_data']['aceso'][-1]],
            "fusee": [json_data_a['X_data']['fusee'][-1]]
        },
        "Y_data": {
            "aceso": [json_data_a['Y_data']['aceso'][-1]],
            "fusee": [json_data_a['Y_data']['fusee'][-1]]
        }
    },
    {
        "methods": ["fusee", "aceso"],
        "X_data": {
            "aceso": [json_data_b['X_data']['aceso'][-1]],
            "fusee": [json_data_b['X_data']['fusee'][-1]]
        },
        "Y_data": {
            "aceso": [json_data_b['Y_data']['aceso'][-1]],
            "fusee": [json_data_b['Y_data']['fusee'][-1]]
        }
    },
    {
        "methods": ["fusee", "aceso"],
        "X_data": {
            "aceso": [json_data_c['X_data']['aceso'][-1]],
            "fusee": [json_data_c['X_data']['fusee'][-1]]
        },
        "Y_data": {
            "aceso": [json_data_c['Y_data']['aceso'][-1]],
            "fusee": [json_data_c['Y_data']['fusee'][-1]]
        }
    },
    {
        "methods": ["fusee", "aceso"],
        "X_data": {
            "aceso": [json_data_d['X_data']['aceso'][-1]],
            "fusee": [json_data_d['X_data']['fusee'][-1]]
        },
        "Y_data": {
            "aceso": [json_data_d['Y_data']['aceso'][-1]],
            "fusee": [json_data_d['Y_data']['fusee'][-1]]
        }
    }
]

my_main_colors = {
    'blue': '#C8C8FF',
    'green': '#A0E6B4',
    'grey': '#ACDCD7',
    'milk': '#FFD7AF',
    'red': '#FCB0A6',
}
translate = {
    'aceso': 'Aceso',
    'fusee': 'FUSEE',
    'clover': 'Clover'
}
plt.rc("font", size=6)
kv_requests = ["insert", "update", "search", "delete"]
colors = list(my_main_colors.values())
hatches = ['/', '\\\\', 'xx', '++++']
plt.rcParams['hatch.linewidth'] = 0.4

bar_width = 0.4
x_labels = ["YCSB A", "YCSB B", "YCSB C", "YCSB D"]
x_positions = np.arange(len(x_labels))
methods = data_sets[0]["methods"]

fig = plt.figure(figsize=(1.67, 0.9), dpi=300)

plt.ylim(0,28)

throughput_data = {}
for dataset in data_sets:
    for method in dataset["methods"]:
        method_throughput = []
        for y_value in dataset["Y_data"][method]:
            mops_value = y_value / 1000000
            method_throughput.append(mops_value)
        if method in throughput_data:
            throughput_data[method].extend(method_throughput)
        else:
            throughput_data[method] = method_throughput

fusee_bars = []
aceso_bars = []
for i, method in enumerate(methods):
    bars = plt.bar(x_positions + (i+0.5) * bar_width, throughput_data[method], bar_width, label=translate[method], color=blue_colors[i*2],
            alpha=1,
            edgecolor='black', linewidth=0.5,
            hatch='')
    if i == 0:
        fusee_bars = bars

    else:
        aceso_bars = bars

# Add annotations
for fusees, acesos in zip(zip(fusee_bars), zip(aceso_bars)):
    for bar_fus, bar_ace in zip(fusees, acesos):
        height_p50 = bar_fus.get_height()
        height_p99 = bar_ace.get_height()
        plt.annotate(f'{height_p99 / height_p50:.2f}X', xy=(bar_ace.get_x() + bar_ace.get_width() / 2, height_p99), 
                        xytext=(0, 0), textcoords="offset points", ha='center', va='bottom', fontsize=5, rotation=0)

plt.gca().spines['top'].set_linewidth(0.5)
plt.gca().spines['bottom'].set_linewidth(0.5)
plt.gca().spines['left'].set_linewidth(0.5)
plt.gca().spines['right'].set_linewidth(0.5)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

ax = plt.gca()
ax.set_ylabel("Throughput (Mops/s)", labelpad=1)
ax.yaxis.set_label_coords(-0.1,0.45)
plt.tick_params(axis='both', which='both', length=2, width=0.5)
plt.gca().tick_params(axis='x', pad=1)
plt.gca().tick_params(axis='y', pad=1)

plt.gca().set_xticks(x_positions + 1 * bar_width)
plt.gca().set_xticklabels(x_labels)

legend = plt.legend(fontsize=6, ncol=2,  loc='upper right', bbox_to_anchor=(1,1.07))
legend.get_frame().set_facecolor('none')
legend.get_frame().set_linewidth(0)

plt.tight_layout(pad=0.2)
plt.savefig("./figures/fig_10.pdf", format='pdf')
plt.close()