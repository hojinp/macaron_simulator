import argparse
import os


parser = argparse.ArgumentParser(description='Create experiment directories')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
args = parser.parse_args()

traces = ["IBMTrace009", "IBMTrace011", "IBMTrace055"]

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'latency', 'cross-cloud')
tracedir = os.path.join(args.project_directory, 'traces')

# Create Remote baseline directories
basedir = os.path.join(experiment_base_dir, 'remote')
for trace in traces:
    trace_path = os.path.join(tracedir, trace + '.csv')
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))
    with open(os.path.join(basedir, trace, 'workloadConfig.yml'), 'w') as f:
        f.write(f"macaronCacheEnabled: false\ntraceFilename: {trace_path}\n")

# Create Replicated baseline directories
basedir = os.path.join(experiment_base_dir, 'replicated')
for trace in traces:
    trace_path = os.path.join(tracedir, trace + '.csv')
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))
    with open(os.path.join(basedir, trace, 'workloadConfig.yml'), 'w') as f:
        f.write(f"macaronCacheEnabled: false\ntraceFilename: {trace_path}\n")

# Create ECPC baseline directories
basedir = os.path.join(experiment_base_dir, 'ecpc')
for trace in traces:
    trace_path = os.path.join(tracedir, trace + '.csv')
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))
    with open(os.path.join(basedir, trace, 'workloadConfig.yml'), 'w') as f:
        f.write(f"macaronCacheEnabled: true\ntraceFilename: {trace_path}\n")
    with open(os.path.join(basedir, trace, 'macaronCacheConfig.yml'), 'w') as f:
        f.write(f"---\ninclusionPolicy: INCLUSIVE\ncachingPolicy: WRITE-THROUGH\ncacheType: LRU\ncacheNodeCount: 1\ncacheNodeMachineType: r5.xlarge\n")

# Create Macaron baseline directories
basedir = os.path.join(experiment_base_dir, 'macaron')
for trace in traces:
    trace_path = os.path.join(tracedir, trace + '.csv')
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))
    with open(os.path.join(basedir, trace, 'workloadConfig.yml'), 'w') as f:
        f.write(f"macaronCacheEnabled: true\ntraceFilename: {trace_path}\n")
    with open(os.path.join(basedir, trace, 'macaronCacheConfig.yml'), 'w') as f:
        f.write("---\ninclusionPolicy: INCLUSIVE\ncachingPolicy: WRITE-THROUGH\noscCacheType: LRU\npacking: true\noscCapacity: 100000\n")

# Create Macaron+CC baseline directories
basedir = os.path.join(experiment_base_dir, 'macaron-cc')
for trace in traces:
    trace_path = os.path.join(tracedir, trace + '.csv')
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))
    with open(os.path.join(basedir, trace, 'workloadConfig.yml'), 'w') as f:
        f.write(f"macaronCacheEnabled: true\ntraceFilename: {trace_path}\n")
    with open(os.path.join(basedir, trace, 'macaronCacheConfig.yml'), 'w') as f:
        f.write("---\ninclusionPolicy: INCLUSIVE\ncachingPolicy: WRITE-THROUGH\ncacheType: LRU\noscCacheType: LRU\n" + 
                "packing: true\noscCapacity: 100000\ncacheNodeCount: 1\ncacheNodeMachineType: r5.xlarge\n")

