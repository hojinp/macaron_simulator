import argparse
import os


parser = argparse.ArgumentParser(description='Create experiment directories')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-c', '--cross', type=str, required=True, help='cross-cloud/region/10p/1p', choices=['cross-cloud', 'cross-region', 'cross-10p', 'cross-1p'])
args = parser.parse_args()

traces = [
    "Uber1", "Uber2", "Uber3",
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs', args.cross)
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

# Create Oracular baseline directories
basedir = os.path.join(experiment_base_dir, 'oracular')
for trace in traces:
    if os.path.exists(os.path.join(basedir, trace)):
        print(f"Experiment directory for {trace} already exist. Skipping...")
        continue
    os.makedirs(os.path.join(basedir, trace))

