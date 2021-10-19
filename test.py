# %%
import os
from typing import List

# import paramiko
import numpy as np
import matplotlib.pyplot as plt

from cost_estimator import Loader, Preprocessor, RegressionModels, Task
from numpy.core.records import array

# paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = pow(2, 22) # 4MB per chunk

def make_path(p):
    if not os.path.exists(p):
        os.makedirs(p)
    return p

def check_betteRTE():
    under_stat = ['throughput.csv', 'job-0-timeline.csv']
    under_folder = ['']
    # base_path = '/opt/shared-disk2/db_betterRTE/'
    base_path = '/opt/shared-disk2/db_betterRTE_ver2/'
    round_range = range(0, 3)
    for round in round_range:
        print(f"Round {round}")
        round_path = os.path.join(base_path, f'round{round}')
        rte_range = [1] + list(range(5, 201, 5))

        # Check MMG
        for type in ['mmt', 'mmg']:
            type_path = os.path.join(round_path, type)
            for rte in rte_range:
                bench_path = os.path.join(type_path, f'{type}_rte-{rte}', 'reports')
                # print(f"Benchmark path: {bench_path}")
                is_completed = True
                # Check benchmark path
                if os.path.isdir(bench_path):
                    # server_count = 4
                    # ['transaction-cpu-time-server-', 'transaction-diskio-count-server-', 'transaction-latency-server-', ]

                    # Check stats path
                    stats_path = os.path.join(bench_path, 'stats')
                    if os.path.isdir(stats_path):
                        throughput = os.path.join(stats_path, 'throughput.csv')
                        timeline = os.path.join(stats_path, 'job-0-timeline.csv')
                        
                        if not os.path.isfile(throughput):
                            is_completed = False
                        if not os.path.isfile(timeline):
                            is_completed = False
                    else:
                        is_completed = False
                else:
                    is_completed = False
            
                if not is_completed:
                    print(f"Round {round}/{type}_rte-{rte} doesn't exist.")
    
def save_non_dependent_ous(npy_path: str, workload_type: str, server_count: int, warmup_sec: int, ous: List, rounds: List, rtes: List, n_rows: int=None):
    # npy_path = 'temp/betterRTE/npy'
    # workload_type = 'mmg'
    # server_count = 4
    # warmup_sec = 90
    # ous = ['OU1 - Generate Plan', 'OU2 - Initialize Thread', 'OU3 - Acquire Locks', 'OU4 - Read from Local']
    # rounds = [0, 1]
    # rtes = [1] + list(range(5, 6, 5))
    # n_rows = 10

    for ou in ous:
        ou_path = make_path(os.path.join(npy_path))
        ou_latency_mean_sum = 0
        ou_latency_std_sum = 0
        ou_latency_means = []
        ou_latency_stds = []
        
        for rte in rtes:
            for round in rounds:
                print(f"RTE {rte} | Round {round}")
                loader = Loader(f'/opt/shared-disk2/db_betterRTE_ver2/round{round}/{workload_type}/{workload_type}_rte-{rte}/reports/', 
                                server_count=server_count, n_jobs=8, n_rows=n_rows)

                df_features = loader.load_features_as_df(auto_save=True, load_from_pkl=True)
                df_latencies = loader.load_latencies_as_df(auto_save=True, load_from_pkl=True)
                psr = Preprocessor(df_features, df_latencies, ou, warmup_interval_second=warmup_sec)
                # psr.drop_warmup_data()

                ou_latency_mean_sum += float(psr.get_label().mean())
                ou_latency_std_sum += float(psr.get_label().std())
                # print(f"OU Latency Mean Sum: {ou_latency_mean_sum}")
                # print(f"OU Latency Std Sum: {ou_latency_std_sum}")

            ou_latency_mean = ou_latency_mean_sum / len(rounds)
            ou_latency_std = ou_latency_std_sum / len(rounds)
            # print(f"OU Latency Mean: {ou_latency_mean}")
            # print(f"OU Latency Std: {ou_latency_std}")
            ou_latency_means.append(ou_latency_mean)
            ou_latency_stds.append(ou_latency_std)
            # print(f"OU Latency Means: {ou_latency_means}")
            # print(f"OU Latency Stds: {ou_latency_stds}")
        
        ou_latency_means_np = np.array(ou_latency_means)
        ou_latency_stds_np = np.array(ou_latency_stds)
        # print(f"OU Latency Means NP: {ou_latency_means_np}")
        # print(f"OU Latency Stds NP: {ou_latency_stds_np}")
        ou_latency = np.stack((ou_latency_means_np, ou_latency_stds_np))
        # print(f"OU Latency: {ou_latency}")        
        np.save(os.path.join(ou_path, f"{ou}.npy"), ou_latency)

def draw_non_dependent_ous(npy_path: str, workload_type: str, server_count: str, warmup_sec: int, ous: List, rounds: List, rtes: List, n_rows: int=None):
    npy_path = 'temp/betterRTE/npy'
    ous = ['OU1 - Generate Plan', 'OU2 - Initialize Thread', 'OU3 - Acquire Locks', 'OU4 - Read from Local']
    os_chart_path = make_path(os.path.join("temp/betterRTE/chart"))

    for ou in ous:
        ou_data = np.load(os.path.join(npy_path, f"{ou}.npy"))
        print(ou_data)

        # Draw chart
        plt.figure(figsize=(9, 6))
        plt.plot(rtes, ou_data[0])
        plt.title(f"Mean Latency Of {ou} Along The Number Of RTEs On {workload_type} With {server_count} Servers")
        plt.xlabel("Num of RTEs")
        plt.ylabel("Mean Latency of the OU")
        plt.savefig(os.path.join(os_chart_path, f"{ou}.png"))

# %%
if __name__ == '__main__':
    # check_betteRTE()
    npy_path = 'temp/betterRTE/npy'
    workload_type = 'mmt'
    server_count = 4
    warmup_sec = 90
    ous = ['OU1 - Generate Plan', 'OU2 - Initialize Thread', 'OU3 - Acquire Locks', 'OU4 - Read from Local']
    rounds = [0, 1, 2]
    # rounds = [0]
    rtes = [1] + list(range(5, 201, 5))
    n_rows = None
    
    save_non_dependent_ous(npy_path=npy_path, workload_type=workload_type, server_count=server_count, warmup_sec=warmup_sec, ous=ous, rounds=rounds, rtes=rtes, n_rows=n_rows)
    draw_non_dependent_ous(npy_path=npy_path, workload_type=workload_type, server_count=server_count, warmup_sec=warmup_sec, ous=ous, rounds=rounds, rtes=rtes, n_rows=n_rows)

# %%
