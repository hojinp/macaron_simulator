import argparse
import os

from matplotlib import pyplot as plt
plt.rcParams.update({'font.size': 25})

TB_TO_MB = 1024. * 1024.
TRACE_LENGTH = 7 * 24 * 60

parser = argparse.ArgumentParser(description='Draw graphs for cost comparison')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

trace_to_osc = {"055": 1.8, "083": 52}
colors = ['#d55e00', '#56b4e9', '#cc79a7']
traces = ["IBMTrace_055_083", "IBMTrace_083_055"]
decay_targets = [f"{v:.4f}" for v in [0.2, 0.1]]

            
for idx, trace in enumerate(traces):
    experiment_directory = os.path.join(args.project_directory, 'scripts', 'results', 'adaptivity', trace)
    
    nodecay, decays = [], {}
    filename = os.path.join(experiment_directory, f"optimization_nodecay.csv")
    with open(filename) as file:
        for line in file:
            if line.startswith("minute"):
                continue
            splits = line.split(',')
            nodecay.append((int(splits[0]), float(splits[1]) / TB_TO_MB))
    for decay_target in decay_targets:
        filename = os.path.join(experiment_directory, f"optimization_{decay_target}.csv")
        decay = []
        with open(filename) as file:
            for line in file:
                if line.startswith("minute"):
                    continue
                splits = line.split(',')
                decay.append((int(splits[0]), float(splits[1]) / TB_TO_MB))
        decays[decay_target] = decay
    
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot([x / 60. for x, _ in nodecay], [y for _, y in nodecay], label="No Decay", color=colors[0], linewidth=3)
    for i, decay_target in enumerate(decay_targets):
        ax.plot([x / 60. for x, _ in decays[decay_target]], [y for _, y in decays[decay_target]], 
                label=f"DF: {float(decay_target)}", color=colors[i + 1], linewidth=3, 
                linestyle='dotted' if i == 0 else 'dashed')
        
    ymax=80
    ax.set_ylim(ymin=0, ymax=ymax * 1.1)
    ax.set_xlim(xmin=0)
    ax.set_xlabel("Time (hr)")
    ax.set_ylabel("OSC size (TB)")

    trace1, trace2 = trace[9:12], trace[13:16]
    if trace1 == "055" and trace2 == "083":
        ax.legend(frameon=False, loc='upper center', bbox_to_anchor=(0.27, 0.7))
    else:
        ax.legend(frameon=False)
    ax.fill_between([1440 / 60., TRACE_LENGTH / 60.], 0, ymax * 1.1, color='lightgrey', alpha=0.5)
    
    ax.text(1440 / 60. / 2., ymax / 2, "observation period", color='black', rotation=90, verticalalignment='center', horizontalalignment='center')
    ax.text(TRACE_LENGTH / 60. / 2., ymax * 1.05, f"IBM {trace1}\n(Opt. OSC: {trace_to_osc[trace1]}TB)", color='black', verticalalignment='top', horizontalalignment='center')
    ax.axvline(x=1440 / 60., color='grey', linestyle='--')
    ax.text(TRACE_LENGTH / 60. + TRACE_LENGTH / 60. / 2., ymax * 1.05, f"IBM {trace2}\n(Opt. OSC: {trace_to_osc[trace2]}TB)", color='black', verticalalignment='top', horizontalalignment='center')
    ax.axvline(TRACE_LENGTH / 60., color='red', linestyle='--')
    fig.savefig(os.path.join(args.project_directory, 'scripts', 'figure8', 'figure8a.png' if idx == 0 else 'figure8b.png'), bbox_inches='tight')
    print(f"Figure is saved at {os.path.join(args.project_directory, 'scripts', 'figure8', 'figure8a.png' if idx == 0 else 'figure8b.png')}")


