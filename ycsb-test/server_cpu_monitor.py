import psutil
import time
import json
import threading
import os

current_directory = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(current_directory, 'server_cpu_ratio.json')

stop_event = threading.Event()
core_map = {
    'rpc_core' : 0,
    'encode_core' : 1,
    'send_core' : 2,
    'recv_core' : 3
}

def record_cpu_usage():
    data_labels = ['rpc_core', 'encode_core', 'send_core', 'recv_core']
    plot_data = {
        'methods': data_labels,
        'X_data': [],
        'Y_data': {data_label: [] for data_label in data_labels},
    }

    total_time = 0
    while not stop_event.is_set():
        cpu_usages = psutil.cpu_percent(interval=0.2, percpu=True)
        total_time += 0.2
        plot_data['X_data'].append(round(total_time, 1))
        for data_label in data_labels:
            plot_data['Y_data'][data_label].append(cpu_usages[core_map[data_label]])

    with open(data_file_path, 'w') as f:
        json.dump(plot_data, f)

if __name__ == "__main__":
    record_thread = threading.Thread(target=record_cpu_usage)
    record_thread.start()

    time.sleep(25)

    stop_event.set()
    record_thread.join()
