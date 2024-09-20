# currently unused
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import json

data_path = "./results"
with (Path(data_path) / f'tab_3a.json').open(mode='r') as f:
    json_data = json.load(f)
core_labels = ['rpc_core', 'encode_core', 'send_core', 'recv_core']
for core in core_labels:
    core_usage = json_data["Y_data"][core]
    
    first_non_zero_index = next(i for i, value in enumerate(core_usage) if value != 0)
    last_non_zero_index = len(core_usage) - next(i for i, value in enumerate(reversed(core_usage)) if value != 0) - 1

    non_zero_values = core_usage[first_non_zero_index:last_non_zero_index + 1]
    mean_value = sum(non_zero_values) / len(non_zero_values)
    
    print(f"avg {core}\t core usage: ", mean_value)

# avg rpc_core     core usage:  3.7972727272727287
# avg encode_core  core usage:  41.877358490566046
# avg send_core    core usage:  29.09537037037037
# avg recv_core    core usage:  43.07169811320756