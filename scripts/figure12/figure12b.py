import argparse
import os

from matplotlib import pyplot as plt
import pandas as pd

import matplotlib
matplotlib.rcParams.update({'font.size': 40})

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

remote_color = "#A07A19"
replicated_color = "#AC30C0"
ecpc_color = "#EB9A72"
macaron_no_dram_color = "#009e73"
macaron_color = "#EA22A8"

VM = 0.252 # r5.xlarge

RETENTION_PERIOD = 90
TRANSFER_COST_PER_GB = 0.09
PUT_OP_COST, GET_OP_COST = 0.000005, 0.0000004
STORAGE_COST_PER_HR = 0.023 / 30.5 / 24.
GB = 1024. * 1024. * 1024.


traces = [
    "Uber1", "Uber2", "Uber3",
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs', 'cross-cloud')

def compute_remote(trace, beg_hr, end_hr):
    cost_filepath = os.path.join(experiment_base_dir, 'remote', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    op = df[df['Component'].str.startswith('DATALAKE')][['PutDatalake', 'GetDatalake']].sum().sum()
    ret = (egress, 0, op, 0)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

def compute_replicated(trace, beg_hr, end_hr, DARK_PORTION):
    workload_dirpath = os.path.join(args.project_directory, 'scripts', 'data', 'workload_analysis', trace)
    load_filepath, wss_file, wss_wo_del_file = os.path.join(workload_dirpath, 'load.csv'), os.path.join(workload_dirpath, 'wss.csv'), os.path.join(workload_dirpath, 'wss_no_del.csv')
    load_df, wss_df, wss_wo_del_df = pd.read_csv(load_filepath), pd.read_csv(wss_file), pd.read_csv(wss_wo_del_file)
    wss_wo_del_df = wss_wo_del_df[(wss_wo_del_df['HR'] > beg_hr) & (wss_wo_del_df['HR'] <= end_hr)]
    egress = (wss_wo_del_df['WSS'].max() - wss_wo_del_df['WSS'].min()) / GB * TRANSFER_COST_PER_GB / (1. - DARK_PORTION)
    capacity = wss_df['WSS'].max() / GB / (end_hr / 24.) * RETENTION_PERIOD / (1. - DARK_PORTION) * STORAGE_COST_PER_HR * (end_hr - beg_hr)
    load_df = load_df[(load_df['HR'] > beg_hr) & (load_df['HR'] <= end_hr)]
    op = load_df['PutCount'].sum() * PUT_OP_COST * 2 + load_df['GetCount'].sum() * GET_OP_COST
    ret = (egress, capacity, op, 0)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

def compute_macaron(trace, beg_hr, end_hr):
    cost_filepath = os.path.join(experiment_base_dir, 'macaron', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    capacity = df[df["Component"].str.startswith("OSC")]["Capacity"].sum()
    op = df[df["Component"].str.startswith("OSC")][["PutOSC", "GCPutOSC", "GCGetOSC"]].sum().sum()
        
    lambda_filepath = os.path.join(args.project_directory, 'scripts', 'data', 'lambda_costs.csv')
    lambda_df = pd.read_csv(lambda_filepath)
    lambda_cost = lambda_df[lambda_df['Trace'] == trace]['Cost'].values[0]
    infra = VM * (end_hr - beg_hr) + lambda_cost
    ret = (egress, capacity, op, infra)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

all_remote_op = 0
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    all_remote_op += compute_remote(trace, beg_hr, end_hr)[2]
    
macaron = all_remote_op
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    macaron += sum(compute_macaron(trace, beg_hr, end_hr))
    
# portions = [0.99, 0.95, 0.70, 0.40, 0.0]
replicated_99, replicated_95, replicated_70, replicated_40, replicated_0 = 0., 0., 0., 0., 0.
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    replicated_99 += sum(compute_replicated(trace, beg_hr, end_hr, 0.99))
    replicated_95 += sum(compute_replicated(trace, beg_hr, end_hr, 0.95))
    replicated_70 += sum(compute_replicated(trace, beg_hr, end_hr, 0.70))
    replicated_40 += sum(compute_replicated(trace, beg_hr, end_hr, 0.40))
    replicated_0 += sum(compute_replicated(trace, beg_hr, end_hr, 0.0))

# compute remote
def compute_remote(dirpath, beg_hr, end_hr, amp):
    cost_filepath = os.path.join(dirpath, "cost.log")
    assert os.path.exists(cost_filepath), f"{cost_filepath} does not exist"
    beg_min, end_min = beg_hr * 60, end_hr * 60
    
    df = pd.read_csv(cost_filepath)
    df = df[(df["Time(min)"] > beg_min) & (df["Time(min)"] <= end_min)]
    egress = df[df["Component"].str.startswith("DATALAKE")][["Transfer2App", "Transfer2DL"]].sum().sum() 
    op = df[df["Component"].str.startswith("DATALAKE")][["PutDatalake", "GetDatalake"]].sum().sum()
    return egress * amp, op * amp

ratios = [replicated_99 / macaron, replicated_95 / macaron, replicated_70 / macaron, replicated_40 / macaron, replicated_0 / macaron]

# draw four sets of three bar charts (remote, replicated, macaron) for (cloud, region, 10p, 1p)
width = 0.8
fig, ax = plt.subplots(1, 1, figsize=(10, 6))
ax.bar([0, 1, 2, 3, 4], ratios, width, color='#D4D4D4', edgecolor='black')
ax.text(0, ratios[0], f"{ratios[0]:.1f}", ha='center', va='bottom', fontsize=20)
ax.text(1, ratios[1], f"{ratios[1]:.1f}", ha='center', va='bottom', fontsize=20)
ax.text(2, ratios[2], f"{ratios[2]:.1f}", ha='center', va='bottom', fontsize=20)
ax.text(3, ratios[3], f"{ratios[3]:.1f}", ha='center', va='bottom', fontsize=20)
ax.text(4, ratios[4], f"{ratios[4]:.1f}", ha='center', va='bottom', fontsize=20)
ax.set_xlabel('Dark data portion', fontsize=30)
ax.set_ylabel('Cost ratio to Macaron', fontsize=30)
ax.set_ylim(ymin=0, ymax=180)

ax.set_xticks([0, 1, 2, 3, 4])
ax.set_xticklabels(["99%", "95%", "70%", "40%", "0%"], fontsize=30)

fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure12', 'figure12b.png'), bbox_inches='tight')

