import argparse
import os

from matplotlib import pyplot as plt
import pandas as pd

import matplotlib
import matplotlib.patches as mpatches
matplotlib.rcParams.update({'font.size': 40})

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

remote_hatch, remote_color = "x", "#D8C570"
replicated_hatch, replicated_color = "+", "#C75A6C"
ecpc_hatch, ecpc_color = "//", "#088038"
macaron_hatch, macaron_color = "\\", "#7E1E4B"
oracular_hatch, oracular_color = "o", "#333333"

VM = 0.252 # r5.xlarge

RETENTION_PERIOD = 90
PUT_OP_COST, GET_OP_COST = 0.000005, 0.0000004
STORAGE_COST_PER_HR = 0.023 / 30.5 / 24.
DARK_PORTION = 0.7
GB = 1024. * 1024. * 1024.

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

traces = [
    "Uber1", "Uber2", "Uber3",
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs')

def compute_remote(trace, beg_hr, end_hr, cross):
    cost_filepath = os.path.join(experiment_base_dir, cross, 'remote', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    op = df[df['Component'].str.startswith('DATALAKE')][['PutDatalake', 'GetDatalake']].sum().sum()
    ret = (egress, 0, op, 0)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

def compute_replicated(trace, beg_hr, end_hr, cross):
    TRANSFER_COST_PER_GB = 0.09 if cross == 'cross-cloud' else 0.02 if cross == 'cross-region' else 0.009 if cross == 'cross-10p' else 0.0009
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

def compute_ecpc(trace, beg_hr, end_hr, cross):
    cost_filepath = os.path.join(experiment_base_dir, cross, 'ecpc', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    capacity = df[df['Component'].str.startswith('DRAM')]['Machine'].sum()
    infra = VM * (end_hr - beg_hr)
    ret = (egress, capacity, 0, infra)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

def compute_macaron(trace, beg_hr, end_hr, cross):
    cost_filepath = os.path.join(experiment_base_dir, cross, 'macaron', trace, 'output', 'cost.log')
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

def compute_oracular(trace, beg_hr, end_hr, cross):
    cost_filepath = os.path.join(experiment_base_dir, cross, 'oracular', trace, 'cost.csv')
    df = pd.read_csv(cost_filepath)
    df = df[(df['HR'] > beg_hr) & (df['HR'] <= end_hr)]
    egress = df[['DTC2APP', 'DTC2DL']].sum().sum()
    capacity = df['CapacityCost'].sum()
    infra = VM * (end_hr - beg_hr)
    ret = (egress, capacity, 0, infra)
    if trace.startswith('Uber'):
        ret = (ret[0] * 100, ret[1] * 100, ret[2] * 100, ret[3])
    return ret

remote_cloud, remote_region, remote_10p, remote_1p = 0, 0, 0, 0
all_remote_op = 0
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    costs = compute_remote(trace, beg_hr, end_hr, 'cross-cloud')
    all_remote_op += costs[2]
    remote_cloud += sum(costs)
    remote_region += sum(compute_remote(trace, beg_hr, end_hr, 'cross-region'))
    remote_10p += sum(compute_remote(trace, beg_hr, end_hr, 'cross-10p'))
    remote_1p += sum(compute_remote(trace, beg_hr, end_hr, 'cross-1p'))

replicated_cloud, replicated_region, replicated_10p, replicated_1p = 0, 0, 0, 0
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    replicated_cloud += sum(compute_replicated(trace, beg_hr, end_hr, 'cross-cloud'))
    replicated_region += sum(compute_replicated(trace, beg_hr, end_hr, 'cross-region'))
    replicated_10p += sum(compute_replicated(trace, beg_hr, end_hr, 'cross-10p'))
    replicated_1p += sum(compute_replicated(trace, beg_hr, end_hr, 'cross-1p'))
    
ecpc_cloud, ecpc_region, ecpc_10p, ecpc_1p = all_remote_op, all_remote_op, all_remote_op, all_remote_op
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    ecpc_cloud += sum(compute_ecpc(trace, beg_hr, end_hr, 'cross-cloud'))
    ecpc_region += sum(compute_ecpc(trace, beg_hr, end_hr, 'cross-region'))
    ecpc_10p += sum(compute_ecpc(trace, beg_hr, end_hr, 'cross-10p'))
    ecpc_1p += sum(compute_ecpc(trace, beg_hr, end_hr, 'cross-1p'))
    
macaron_cloud, macaron_region, macaron_10p, macaron_1p = all_remote_op, all_remote_op, all_remote_op, all_remote_op
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    macaron_cloud += sum(compute_macaron(trace, beg_hr, end_hr, 'cross-cloud'))
    macaron_region += sum(compute_macaron(trace, beg_hr, end_hr, 'cross-region'))
    macaron_10p += sum(compute_macaron(trace, beg_hr, end_hr, 'cross-10p'))
    macaron_1p += sum(compute_macaron(trace, beg_hr, end_hr, 'cross-1p'))
    
oracular_cloud, oracular_region, oracular_10p, oracular_1p = all_remote_op, all_remote_op, all_remote_op, all_remote_op
for trace in traces:
    beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
    oracular_cloud += sum(compute_oracular(trace, beg_hr, end_hr, 'cross-cloud'))
    oracular_region += sum(compute_oracular(trace, beg_hr, end_hr, 'cross-region'))
    oracular_10p += sum(compute_oracular(trace, beg_hr, end_hr, 'cross-10p'))
    oracular_1p += sum(compute_oracular(trace, beg_hr, end_hr, 'cross-1p'))

width = 0.15
fig, ax = plt.subplots(1, 1, figsize=(15, 9))
ax.bar([i for i in range(4)], [remote_cloud, remote_region, remote_10p, remote_1p], width, color=remote_color, label="REMOTE", hatch=remote_hatch)
ax.bar([i + width for i in range(4)], [replicated_cloud, replicated_region, replicated_10p, replicated_1p], width, color=replicated_color, label="REPLICATED", hatch=replicated_hatch)
ax.bar([i + 2 * width for i in range(4)], [ecpc_cloud, ecpc_region, ecpc_10p, ecpc_1p], width, color=ecpc_color, label="ECPC", hatch=ecpc_hatch)
ax.bar([i + 3 * width for i in range(4)], [macaron_cloud, macaron_region, macaron_10p, macaron_1p], width, color=macaron_color, label="Macaron", hatch=macaron_hatch)
ax.bar([i + 4 * width for i in range(4)], [oracular_cloud, oracular_region, oracular_10p, oracular_1p], width, color="black", label="Oracular", hatch=oracular_hatch)
ax.set_xlabel('Ratio to cross-cloud egress cost (%)', fontsize=40)
ax.set_ylabel('Cost ($)', fontsize=40)

def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '%d%s' % (num, ['', 'K', 'M', 'B', 'T'][magnitude])

xticknames = ["100%\n(x-cloud)", "22%\n(x-region)", "10%", "1%"]
ax.set_xticks([idx + width for idx in range(4)])
ax.set_xticklabels(xticknames)
current_yticks = ax.get_yticks()
ax.set_yticks(current_yticks, [human_format(val) for val in current_yticks])

custom_handles = [
    mpatches.Patch(facecolor=remote_color, label="Remote", hatch=remote_hatch),
    mpatches.Patch(facecolor=ecpc_color, label="ECPC", hatch=ecpc_hatch),
    mpatches.Patch(facecolor="black", label="Oracular", hatch=oracular_hatch),
    mpatches.Patch(facecolor=replicated_color, label="Replicated", hatch=replicated_hatch),
    mpatches.Patch(facecolor=macaron_color, label="Macaron", hatch=macaron_hatch),
]
ax.legend(handles=custom_handles, ncol=2, frameon=False, loc='upper right', fontsize=40, columnspacing=0.4)
fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure12', 'figure12a.png'), bbox_inches='tight')

