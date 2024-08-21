import argparse
import os
import time

OPTS = '-Xmx100g -ea'
EXECCLASS = 'edu.cmu.pdl.macaronsimulator.simulator.runner.SimulatorRunner'
CROSS_REGION = 'use1-usw1'

parser = argparse.ArgumentParser(description='Run evaluation experiments')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-j', '--jarfile_path', type=str, required=True, help='Path to the jar file')
parser.add_argument('-t', '--trace', type=str, help='Trace to evaluate')
parser.add_argument('-m', '--method', type=str, help='Approach to evaluate (including baselines)', choices=['remote', 'replicated', 'ecpc', 'macaron', 'macaron-cc'])
args = parser.parse_args()

methods = ['remote', 'replicated', 'ecpc', 'macaron', 'macaron-cc'] if args.method is None else [args.method]
traces = ["IBMTrace009", "IBMTrace011", "IBMTrace055"]

if args.trace is not None:
    assert args.trace in traces, f"Unknown trace: {args.trace}"
    traces = [args.trace]

tracedb_dirname = os.path.join(args.project_directory, 'traces', 'tracedb')
os.makedirs(tracedb_dirname, exist_ok=True)

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'latency', 'cross-cloud')
COST_FILE = os.path.join(args.project_directory, 'scripts', 'data', f'cross-cloud_costs.yml')

target_latencies = {"IBMTrace009": 160092, "IBMTrace011": 148357, "IBMTrace055": 109302}
total_sizes = {"IBMTrace009": 6754800, "IBMTrace011": 3782800, "IBMTrace055": 134537}

for method in methods:
    print(f"Start running experiments for {method}")
    for idx, trace in enumerate(traces):
        start_time = time.time()
        
        experiment_dirpath = os.path.join(experiment_base_dir, method, trace)
        if os.path.exists(os.path.join(experiment_dirpath, 'output')) or os.path.exists(os.path.join(experiment_dirpath, 'cost.csv')):
            print(f"Simulation results for {trace} already exist. Skipping...")
            continue
        
        tracedb_path = os.path.join(tracedb_dirname, trace + '.db')
        miniature_simulation_dirpath = os.path.join(args.project_directory, 'scripts', 'miniature_simulation', trace)
        
        if method == 'remote':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -i -o -d {experiment_dirpath} -p {tracedb_path} -u {CROSS_REGION} -b {COST_FILE} -j -v"
            print(command)
            os.system(command)
            
        elif method == 'replicated':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -i -o -d {experiment_dirpath} -p {tracedb_path} -u {CROSS_REGION} -b {COST_FILE} -j -v -x"
            print(command)
            os.system(command)
            
        elif method == 'ecpc':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -d {experiment_dirpath} -p {tracedb_path} -o -u {CROSS_REGION} " + \
                f"-c {miniature_simulation_dirpath} -b {COST_FILE} -a 15 -e 1440 -g 0.2 -l -j -v"
            print(command)
            os.system(command)
        
        elif method == 'macaron':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -d {experiment_dirpath} -p {tracedb_path} -i -k -u {CROSS_REGION} " + \
                f"-c {miniature_simulation_dirpath} -b {COST_FILE} -a 15 -e 1440 -g 0.2 -j -v"
            print(command)
            os.system(command)
            
        elif method == 'macaron-cc':
            mini_cache_count = min(int(total_sizes[trace] / (26 * 1024)), 200)
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -d {experiment_dirpath} -p {tracedb_path} -u {CROSS_REGION} -r -t {target_latencies[trace]} " + \
                f"-f -c {miniature_simulation_dirpath} -k -b {COST_FILE} -h {mini_cache_count} -a 15 -e 1440 -g 0.2 -j -v"
            print(command)
            os.system(command)
            
        else:
            raise ValueError(f"Unknown method: {method}")
        
        end_time = time.time()
        with open(os.path.join(experiment_dirpath, 'elapsed_time.txt'), 'w') as f:
            f.write(str(end_time - start_time))
