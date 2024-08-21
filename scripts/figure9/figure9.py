import argparse
import os
from matplotlib import pyplot as plt
plt.rcParams.update({'font.size': 20})

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

traces = [
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]

trace_to_data_accessed = {}
for trace in traces:
    wss_wo_del_file = os.path.join(args.project_directory, 'scripts', 'data', 'workload_analysis', trace, 'wss_no_del.csv')
    datasize = 0
    with open(wss_wo_del_file, 'r') as f:
        for line in f:
            if line.startswith("HR"):
                continue
            datasize = max(datasize, int(line.split(',')[1]))
    trace_to_data_accessed[trace] = datasize / 1024. / 1024. / 1024. / 1024.
    
start_minute = 24 * 60
trace_to_capacities = {}
for trace in traces:
    filename = os.path.join(args.project_directory, 'scripts', 'results', 'costs', 'cross-cloud', 'macaron', trace, 'output', 'OSCM-OSCM_SERVER.log')
    capacities = []
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith("Time"):
                continue
            splits = line.split(',')
            minute, capacity = int(splits[0]), int(splits[-1]) / 1024. / 1024. / 1024. / 1024.
            if minute > start_minute:
                capacities.append(capacity)
    trace_to_capacities[trace] = (min(capacities), max(capacities), sum(capacities) / len(capacities))
    
fig = plt.figure(figsize=(5, 5))
fig.subplots_adjust(wspace=0.2)
gs = fig.add_gridspec(1, 2, width_ratios=[1, 2])
ax1 = fig.add_subplot(gs[0])
ax2 = fig.add_subplot(gs[1])

data_accessed = [trace_to_data_accessed[trace] for trace in traces]
min_capacities = [trace_to_capacities[trace][0] for trace in traces]
max_capacities = [trace_to_capacities[trace][1] for trace in traces]
avg_capacities = [trace_to_capacities[trace][2] for trace in traces]

d = .005  # size of the diagonal lines
for i, trace in enumerate(traces): # Plot the first subplot
    if data_accessed[i] <= 20:
        ax1.barh(trace, data_accessed[i], color='#D4D4D4', height=0.8, edgecolor='black')
    else:
        ax1.barh(trace, 20, color='#D4D4D4', height=0.8, edgecolor='black')  # cap the bar at x=20
        kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
        ax1.plot((1 - 2 * d, 1 + 2 * d), (float(i) / len(traces) - 0.4 / len(traces) - d, float(i) / len(traces) - 0.4 / len(traces) + d), **kwargs)
        ax1.plot((1 - 2 * d, 1 + 2 * d), (float(i) / len(traces) + 0.4 / len(traces) - d, float(i) / len(traces) + 0.4 / len(traces) + d), **kwargs)
    ax1.errorbar(avg_capacities[i], trace, xerr=[[avg_capacities[i] - min_capacities[i]], [max_capacities[i] - avg_capacities[i]] if avg_capacities[i] <= 40 else [0]],
                 capsize=4, fmt='o', color='red')

for i, trace in enumerate(traces): # Plot the second subplot
    if data_accessed[i] > 20:
        adjusted_length = max(0, data_accessed[i] - 40)
        ax2.barh(trace, data_accessed[i], color='#D4D4D4', height=0.8, left=0, edgecolor='black')  # start bars at 0
        kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
        ax2.plot((-d, d), (float(i) / len(traces) - 0.4 / len(traces) - d, float(i) / len(traces) - 0.4 / len(traces) + d), **kwargs)
        ax2.plot((-d, d), (float(i) / len(traces) + 0.4 / len(traces) - d, float(i) / len(traces) + 0.4 / len(traces) + d), **kwargs)
    if avg_capacities[i] > 40:
        ax2.errorbar(avg_capacities[i], trace, xerr=[[avg_capacities[i] - min_capacities[i] if min_capacities[i] < 40 else 0], [max_capacities[i] - avg_capacities[i]]],
                     capsize=4, fmt='o', color='red')
    else:
        ax2.barh(trace, 0, color='white', height=0.8, left=0, edgecolor='black')

# Set the x-axis limits manually
ax1.set_xlim([0, 20])
ax2.set_xlim([40, 80])

# Hide the spines between ax and ax2
ax1.spines['right'].set_visible(False)
ax2.spines['left'].set_visible(False)
ax1.yaxis.tick_left()
ax2.yaxis.tick_right()
ax2.tick_params(labelright='off')  # don't put tick labels at the top
ax2.set_yticks([])

ax1.set_ylabel('IBM Trace ID')
ax1.set_xlabel('Size (TB)', fontsize=20)
ax1.xaxis.set_label_coords(1.5, -0.1)

# Adding diagonal lines to denote the break
d = .02  # size of the diagonal lines
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
ax1.plot((1 - 2 * d, 1 + 2 * d), (-d, d), **kwargs)
ax1.plot((1 - 2 * d, 1 + 2 * d), (1 - d, 1 + d), **kwargs)

kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
ax2.plot((-d, +d), (-d, +d), **kwargs)

ax1.set_ylim(-1, len(traces))
ax2.set_ylim(-1, len(traces))

ax1.set_yticks(range(len(traces)))
trace_strings = [f"{int(trace[-3:])}" for trace in traces]
ax1.set_yticklabels(trace_strings)
ax2.set_yticklabels([])


custom_legends = [
    plt.Rectangle((0, 0), 1, 1, fc='#D4D4D4', edgecolor='black'),
    plt.Line2D([0], [0], color='red', marker='o', markersize=5, linestyle='-'),
]
ax2.legend(custom_legends, ["Total data size", "OSC capacity"], loc="center right", frameon=False, fontsize=15)
fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure9', 'figure9.png'), bbox_inches='tight')

