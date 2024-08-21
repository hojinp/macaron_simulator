import argparse
from collections import defaultdict
import os

from matplotlib import pyplot as plt, ticker
plt.rc('font', size=15)
import numpy as np
import pandas as pd

egress_color, egress_hatch = '#f0e442', 'x'
capacity_color, capacity_hatch = '#56b4e9', '+'
op_color, op_hatch = '#009e73', '//'
infra_color, infra_hatch = '#e69f00', 'o'

GB = 1024. * 1024. * 1024.
RETENTION_PERIOD = 90 # days
STORAGE_COST_PER_HR = 0.023 / 30.5 / 24. # per GB
PUT_OP_COST, GET_OP_COST = 0.000005, 0.0000004
DARK_PORTION = 0.7
VM = 0.252 # r5.xlarge

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-c', '--cross', type=str, required=True, help='cross-cloud or cross-region', choices=['cross-cloud', 'cross-region'])
args = parser.parse_args()

TRANSFER_COST_PER_GB = 0.09 if args.cross == 'cross-cloud' else 0.02
traces = ["Uber1", "VMware", "IBMTrace009", "IBMTrace012"]
results = defaultdict(list)

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs', args.cross)

# Returns (egress, capacity, operation, infra) costs
def compute_remote(trace, beg_hr, end_hr):
    cost_filepath = os.path.join(experiment_base_dir, 'remote', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    op = df[df['Component'].str.startswith('DATALAKE')][['PutDatalake', 'GetDatalake']].sum().sum()
    return (egress, 0, op, 0)

def compute_replicated(trace, beg_hr, end_hr):
    workload_dirpath = os.path.join(args.project_directory, 'scripts', 'data', 'workload_analysis', trace)
    load_filepath, wss_file, wss_wo_del_file = os.path.join(workload_dirpath, 'load.csv'), os.path.join(workload_dirpath, 'wss.csv'), os.path.join(workload_dirpath, 'wss_no_del.csv')
    load_df, wss_df, wss_wo_del_df = pd.read_csv(load_filepath), pd.read_csv(wss_file), pd.read_csv(wss_wo_del_file)
    wss_wo_del_df = wss_wo_del_df[(wss_wo_del_df['HR'] > beg_hr) & (wss_wo_del_df['HR'] <= end_hr)]
    egress = (wss_wo_del_df['WSS'].max() - wss_wo_del_df['WSS'].min()) / GB * TRANSFER_COST_PER_GB / (1. - DARK_PORTION)
    capacity = wss_df['WSS'].max() / GB / (end_hr / 24.) * RETENTION_PERIOD / (1. - DARK_PORTION) * STORAGE_COST_PER_HR * (end_hr - beg_hr)
    load_df = load_df[(load_df['HR'] > beg_hr) & (load_df['HR'] <= end_hr)]
    op = load_df['PutCount'].sum() * PUT_OP_COST * 2 + load_df['GetCount'].sum() * GET_OP_COST
    return (egress, capacity, op, 0)

def compute_ecpc(trace, beg_hr, end_hr, op):
    cost_filepath = os.path.join(experiment_base_dir, 'ecpc', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    capacity = df[df['Component'].str.startswith('DRAM')]['Machine'].sum()
    infra = VM * (end_hr - beg_hr)
    return (egress, capacity, op, infra)

def compute_ttl(trace, beg_hr, end_hr, op):
    cost_filepath = os.path.join(experiment_base_dir, 'macaron-ttl', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    capacity = df[df['Component'].str.startswith('OSC')]['Capacity'].sum()
    op = op + df[df['Component'].str.startswith('OSC')][['PutOSC', 'GCPutOSC', 'GCGetOSC']].sum().sum()
    infra = VM * (end_hr - beg_hr)
    return (egress, capacity, op, infra)

def compute_macaron(trace, beg_hr, end_hr, op):
    cost_filepath = os.path.join(experiment_base_dir, 'macaron', trace, 'output', 'cost.log')
    beg_min, end_min = beg_hr * 60, end_hr * 60
    df = pd.read_csv(cost_filepath)
    df = df[(df['Time(min)'] > beg_min) & (df['Time(min)'] <= end_min)]
    egress = df[df['Component'].str.startswith('DATALAKE')][['Transfer2App', 'Transfer2DL']].sum().sum()
    capacity = df[df["Component"].str.startswith("OSC")]["Capacity"].sum()
    op = op + df[df["Component"].str.startswith("OSC")][["PutOSC", "GCPutOSC", "GCGetOSC"]].sum().sum()
        
    lambda_filepath = os.path.join(args.project_directory, 'scripts', 'data', 'lambda_costs.csv')
    lambda_df = pd.read_csv(lambda_filepath)
    lambda_cost = lambda_df[lambda_df['Trace'] == trace]['Cost'].values[0]
    infra = VM * (end_hr - beg_hr) + lambda_cost
    return (egress, capacity, op, infra)

def compute_oracular(trace, beg_hr, end_hr, op):
    cost_filepath = os.path.join(experiment_base_dir, 'oracular', trace, 'cost.csv')
    df = pd.read_csv(cost_filepath)
    df = df[(df['HR'] > beg_hr) & (df['HR'] <= end_hr)]
    egress = df[['DTC2APP', 'DTC2DL']].sum().sum()
    capacity = df['CapacityCost'].sum()
    infra = VM * (end_hr - beg_hr)
    return (egress, capacity, op, infra)

def compute_costs():
    for trace in traces:
        if trace.startswith('VMware'):
            continue
        beg_hr, end_hr = (24, 168) if trace.startswith('IBM') else (24, 408) if trace.startswith('Uber') else (24, 192)
        results[trace].append(compute_remote(trace, beg_hr, end_hr))
        all_remote_op = results[trace][0][2]
        results[trace].append(compute_replicated(trace, beg_hr, end_hr))
        results[trace].append(compute_ecpc(trace, beg_hr, end_hr, all_remote_op))
        results[trace].append(compute_macaron(trace, beg_hr, end_hr, all_remote_op))
        results[trace].append(compute_oracular(trace, beg_hr, end_hr, all_remote_op))
        
        if trace.startswith('Uber'):
            for i in range(len(results[trace])):
                results[trace][i] = (results[trace][i][0] * 100, results[trace][i][1] * 100, results[trace][i][2] * 100, results[trace][i][3])
        
        
def draw_graphs():
    fig, axes = plt.subplots(1, 4, figsize=(12, 4))
    plt.subplots_adjust(wspace=0)
    categories = np.array(['Remote', 'Replicated', 'ECPC', 'Macaron', 'Oracular'])
    for i, trace in enumerate(traces):
        ax = axes[i]
        if trace.startswith('VMware'):
            fig.delaxes(ax)
            continue

        egress = np.array([results[trace][j][0] for j in range(len(categories))])
        capacity = np.array([results[trace][j][1] for j in range(len(categories))])
        operation = np.array([results[trace][j][2] for j in range(len(categories))])
        infra = np.array([results[trace][j][3] for j in range(len(categories))])
        total = egress + capacity + operation + infra
        
        ax.bar(categories, egress, color=egress_color, hatch=egress_hatch)
        ax.bar(categories, capacity, bottom=egress, color=capacity_color, hatch=capacity_hatch)
        ax.bar(categories, operation, bottom=egress + capacity, color=op_color, hatch=op_hatch)
        ax.bar(categories, infra, bottom=egress + capacity + operation, color=infra_color, hatch=infra_hatch)
        
        ax.set_ylabel('Cost ($)')
        ax.set_ylim(0, max(total) * 1.1)
        yticks = ax.get_yticks()
        convertible = True
        for ytick in yticks:
            if int(ytick / 1000) * 1000 != ytick:
                convertible = False
                break
        if convertible:
            ax.yaxis.set_major_locator(ticker.FixedLocator(yticks))
            ax.set_yticklabels([f'{int(ytick / 1000)}K' for ytick in yticks])

        xticks = ax.get_xticks()
        ax.set_xticks(xticks)
        ax.set_xticklabels(categories, rotation=45, ha='right')
        
        if trace.startswith('IBMTrace'):
            ax.set_title(f'IBM {int(trace[-3:])}')
        elif trace.startswith('Uber'):
            ax.set_title('Uber')
        else:
            ax.set_title(trace)
    
    custom_legends = [
        plt.Rectangle((0, 0), 1, 1, fc=egress_color, hatch=egress_hatch),
        plt.Rectangle((0, 0), 1, 1, fc=capacity_color, hatch=capacity_hatch),
        plt.Rectangle((0, 0), 1, 1, fc=op_color, hatch=op_hatch),
        plt.Rectangle((0, 0), 1, 1, fc=infra_color, hatch=infra_hatch),
    ]
    fig.legend(custom_legends, ['Egress', 'Capacity', 'Operation', 'Infra'], ncol=4, loc='upper center', fontsize=17, 
               frameon=True, bbox_to_anchor=(0.5, 1.12), facecolor='white', edgecolor='black')
    fig.tight_layout()
    
    outfile = os.path.join(args.project_directory, 'scripts', 'figure7', 'figure7a.png' if args.cross == 'cross-region' else 'figure7b.png')
    fig.savefig(outfile, bbox_inches='tight')
    print("File is saved at", outfile)
    
compute_costs()
draw_graphs()
