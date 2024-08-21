import argparse
import os

OPTS = '-Xmx100g -ea'
EXECCLASS = 'edu.cmu.pdl.macaronsimulator.simulator.runner.SimulatorRunner'
CROSS_REGION = 'use1-usw1'

parser = argparse.ArgumentParser(description='Run evaluation experiments')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-j', '--jarfile_path', type=str, help='Path to the jar file')
parser.add_argument('-m', '--method', type=str, help='Approach to evaluate (including baselines)', choices=['remote', 'ecpc', 'macaron', 'oracular'])
parser.add_argument('-c', '--cross', type=str, required=True, help='cross-cloud/region/10p/1p', choices=['cross-cloud', 'cross-region', 'cross-10p', 'cross-1p'])
parser.add_argument('-t', '--trace', type=str, help='Trace to evaluate')
parser.add_argument('-s', '--subset', help='Subset of traces to evaluate for Figure 7 (Uber, IBMTrace009, IBMTrace012)', action='store_true')
args = parser.parse_args()

if args.method is None or (args.method is not None and args.method in ['remote', 'ecpc', 'macaron']):
    assert args.jarfile_path is not None, "Jar file path must be provided for remote, ecpc, and macaron methods"
methods = ['remote', 'ecpc', 'macaron', 'oracular'] if args.method is None else [args.method]

traces = [
    "Uber1", "Uber2", "Uber3",
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]
if args.subset:
    assert args.trace is None, "Cannot specify both --subset and --trace"
    traces = ["Uber1", "IBMTrace009", "IBMTrace012"]
    
if args.trace is not None:
    assert args.trace in traces, f"Unknown trace: {args.trace}"
    traces = [args.trace]

tracedb_dirname = os.path.join(args.project_directory, 'traces', 'tracedb')
os.makedirs(tracedb_dirname, exist_ok=True)

experiment_base_dir = os.path.join(args.project_directory, 'scripts', 'results', 'costs', args.cross)
COST_FILE = os.path.join(args.project_directory, 'scripts', 'data', f'{args.cross}_costs.yml')
ORACULAR_SCRIPT = os.path.join(args.project_directory, 'oracular', 'oracular.py')

for method in methods:
    print(f"Start running experiments for {method}")
    for idx, trace in enumerate(traces):
        experiment_dirpath = os.path.join(experiment_base_dir, method, trace)
        if os.path.exists(os.path.join(experiment_dirpath, 'output')) or os.path.exists(os.path.join(experiment_dirpath, 'cost.csv')):
            print(f"Simulation results for {trace} already exist. Skipping...")
            continue
        
        tracefile_path = os.path.join(args.project_directory, 'traces', trace + '.csv')
        tracedb_path = os.path.join(tracedb_dirname, trace + '.db')
        miniature_simulation_dirpath = os.path.join(args.project_directory, 'scripts', 'miniature_simulation', trace)
        
        if method == 'remote':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -i -o -d {experiment_dirpath} -p {tracedb_path} -u {CROSS_REGION} -b {COST_FILE}"
            print(command)
            os.system(command)
            
        elif method == 'ecpc':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -d {experiment_dirpath} -p {tracedb_path} -o -u {CROSS_REGION} " + \
                f"-c {miniature_simulation_dirpath} -b {COST_FILE} -a 15 -e 1440 -g 0.2 -l"
            print(command)
            os.system(command)
        
        elif method == 'macaron':
            command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -d {experiment_dirpath} -p {tracedb_path} -i -k -u {CROSS_REGION} " + \
                f"-c {miniature_simulation_dirpath} -b {COST_FILE} -a 15 -e 1440 -g 0.2"
            print(command)
            os.system(command)
            
        elif method == 'oracular':
            command = f"python3 {ORACULAR_SCRIPT} -t {tracefile_path} -o {experiment_dirpath} -c {args.cross}"
            print(command)
            os.system(command)
            
        else:
            raise ValueError(f"Unknown method: {method}")
