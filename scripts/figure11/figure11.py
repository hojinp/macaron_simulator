import argparse
import os
import matplotlib
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns
matplotlib.rcParams.update({'font.size': 20})

remote_color = "#D8C570"
replicated_color = "#C75A6C"
ecpc_color = "#088038"
macaron_color = "#7E1E4B"
macaron_cc_color = "#241861"

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'latency', 'cross-cloud')
traces = ["VMware", "IBMTrace009", "IBMTrace011", "IBMTrace055"]
request_counts = {"IBMTrace009": 9315200, "IBMTrace011": 4507644, "IBMTrace055": 4634278}
nsampled = 1000000

def get_latencies(filename):
    latencies, sampling_ratio = [], int(request_counts[trace] / nsampled)
    with open(filename, 'r') as inp:
        for idx, line in enumerate(inp):
            if line.startswith("Minute"):
                continue
            if idx % sampling_ratio == 0:
                splits = line.split(',')
                if int(splits[0]) > 1440:
                    latencies.append(int(splits[1]) / 1000000.)
    return latencies

fig, axes = plt.subplots(2, 2, figsize=(16, 8))
for idx, trace in enumerate(traces):
    ax = axes[idx // 2, idx % 2]
    if trace == "VMware":
        continue
    
    filename = os.path.join(experiment_base_dir, "remote", trace, "output", "app_latency_all.csv")
    remote = get_latencies(filename)
    
    filename = os.path.join(experiment_base_dir, "replicated", trace, "output", "app_latency_all.csv")
    replicated = get_latencies(filename)
    
    filename = os.path.join(experiment_base_dir, "ecpc", trace, "output", "app_latency_all.csv")
    ecpc = get_latencies(filename)
    
    filename = os.path.join(experiment_base_dir, "macaron", trace, "output", "app_latency_all.csv")
    macaron = get_latencies(filename)
    
    filename = os.path.join(experiment_base_dir, "macaron-cc", trace, "output", "app_latency_all.csv")
    macaron_cc = get_latencies(filename)
    
    df1 = pd.DataFrame({'Case': ['REMOTE'] * len(remote), 'Latency': remote})
    df2 = pd.DataFrame({'Case': ['REPLICATED'] * len(replicated), 'Latency': replicated})
    df3 = pd.DataFrame({'Case': ['ECPC'] * len(ecpc), 'Latency': ecpc})
    df4 = pd.DataFrame({'Case': ['MACARON'] * len(macaron), 'Latency': macaron})
    df5 = pd.DataFrame({'Case': ['MACARON+CC'] * len(macaron_cc), 'Latency': macaron_cc})
    df = pd.concat([df1, df2, df3, df4, df5], axis=0)
    
    palette = {'REMOTE': remote_color, 'REPLICATED': replicated_color, 'ECPC': ecpc_color, 'MACARON': macaron_color, 'MACARON+CC': macaron_cc_color}
    vp = sns.violinplot(data=df, ax=ax, x='Case', y='Latency', palette=palette, showmeans=False, inner=None, scale='width')
    vp.set(xticklabels=[])
    vp.set(xlabel=None)
    vp.set_ylabel("Latency (s)")
    ax.set_title(f"IBM {trace[-3:]}", fontweight='bold', va='top', y=1.0, pad=-14, fontsize=30)
    ax.set_ylim(ymin=0, ymax=1.5)
    
    remote_avg, replicated_avg, ecpc_avg, macaron_avg, macaron_cc_avg = \
        sum(remote) / len(remote), sum(replicated) / len(replicated), sum(ecpc) / len(ecpc), \
            sum(macaron) / len(macaron), sum(macaron_cc) / len(macaron_cc)
            
    ax.scatter([0, 1, 2, 3, 4], [remote_avg, replicated_avg, ecpc_avg, macaron_avg, macaron_cc_avg], color="#FFFF00", marker="*", s=810, zorder=3, edgecolor="black")
    
custom_handles = [
    mpatches.Patch(facecolor=remote_color, label="Remote"),
    mpatches.Patch(facecolor=macaron_color, label="Macaron"),
    mpatches.Patch(facecolor=replicated_color, label="Replicated"),
    mpatches.Patch(facecolor=macaron_cc_color, label="Macaron+CC"),
    mpatches.Patch(facecolor=ecpc_color, label="ECPC"),
]
axes[0, 0].legend(handles=custom_handles, loc='upper center', bbox_to_anchor=(1.1, 1.7), ncol=3, fontsize=35, columnspacing=0.3, frameon=False)
fig.subplots_adjust(wspace=0.3)
fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure11', 'figure11.png'), bbox_inches='tight')
