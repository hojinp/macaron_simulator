import argparse
from matplotlib import pyplot as plt
import os
plt.rcParams.update({'font.size': 25})


egress_color = "#f0e442"
capacity_color = "#56b4e9"
op_color = "#009e73"
vm_color = "#e69f00"

traces = ["Uber1", "VMware", "IBMTrace055", "IBMTrace083"]
tgt_minutes = {
    "Uber1": 12960,
    "IBMTrace055": 10080,
    "IBMTrace083": 6500
}

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs', 'cross-cloud', 'macaron')
fig, axes = plt.subplots(2, 2, figsize=(10, 8))
plt.subplots_adjust(wspace=0.35)
for idx, trace in enumerate(traces):
    ax = axes[idx // 2, idx % 2]
    if trace == "VMware":
        continue
    filepath = os.path.join(experiment_base_dir, trace, "output/OSCOptResult.csv")
    
    prv_minute = -1
    with open(filepath, 'r') as f:
        for line in f:
            if line.startswith("Time"):
                continue
            splits = line.split(',')
            minute = int(splits[0])
            if minute > tgt_minutes[trace]:
                break
            if minute > prv_minute:
                prv_minute = minute
                sizes, transfers, operations, capacities = [], [], [], []
            sizes.append(int(splits[1]) / 1024)
            capacities.append(float(splits[2]) * 96)
            transfers.append(float(splits[3]) * 96)
            operations.append(float(splits[4]) * 96)
            
    sums = [capacities[i] + transfers[i] + operations[i] for i in range(len(capacities))]
    maxx = max(sizes)
    sizes = [size / maxx for size in sizes]
    min_index = sums.index(min(sums))
    min_size, min_sum = sizes[min_index], sums[min_index]
   
    ax.fill_between(sizes, [capacities[i] + transfers[i] + operations[i] for i in range(len(sizes))], [capacities[i] + transfers[i] for i in range(len(sizes))], 
                    color=op_color)
    ax.fill_between(sizes, [capacities[i] + transfers[i] for i in range(len(sizes))], [capacities[i] for i in range(len(sizes))], 
                    color=egress_color)
    ax.fill_between(sizes, [capacities[i] for i in range(len(sizes))], 0,
                    color=capacity_color)

    ax.scatter(min_size, min_sum, color='yellow', s=900, edgecolor='black', marker="*")
    if idx == 2:
        ax.set_ylabel("Expected Cost ($/day)")
        ax.set_xlabel("Cache size to total data size (r)")
        ax.xaxis.set_label_coords(1.1, -0.2)
        ax.yaxis.set_label_coords(-0.2, 1.1)
    ax.set_xlim(xmin=0, xmax=1)
    if 'Uber' in trace:
        ax.set_ylim(ymin=0, ymax=40)
    else:
        ax.set_ylim(ymin=0)
        
    if 'Uber' in trace:
        ax.set_title(f"Uber\nr={min_size:.2f}", y=1.0, pad=-60)
    else:
        ax.set_title(f"IBM {int(trace[-2:])}\nr={min_size:.2f}", y=1.0, pad=-60)
fig.delaxes(axes[0, 1])
    
custom_legends = [plt.Line2D([0], [0], color=op_color, lw=15),
                  plt.Line2D([0], [0], color=egress_color, lw=15),
                  plt.Line2D([0], [0], color=capacity_color, lw=15),
                  plt.Line2D([], [], markerfacecolor='yellow', markeredgecolor='black', marker="*", markersize=30, lw=0, markeredgewidth=2)]
ax.legend(custom_legends, ["Operations", "Egress", "Capacity", "Min cost"], loc="upper right", frameon=False, ncol=2, bbox_to_anchor=(0.8, 2.65))
fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure10', 'figure10.png'), bbox_inches='tight')

