
import argparse
import os

OPTS = '-Xmx100g -ea'

MINICACHE_COUNT = 200
SAMPLING_RATIO = 0.05
EXECCLASS = 'edu.cmu.pdl.macaronsimulator.simulator.mrc.CostMiniatureSimulationRunner'

parser = argparse.ArgumentParser(description='Run miniature simulations')
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-j', '--jarfile_path', type=str, required=True, help='Path to the jar file')
parser.add_argument('-m', '--minicache_count', type=int, help='Number of miniature caches')
parser.add_argument('-s', '--sampling_ratio', type=float, help='Sampling ratio')
args = parser.parse_args()

if args.minicache_count is not None:
    MINICACHE_COUNT = args.minicache_count
if args.sampling_ratio is not None:
    SAMPLING_RATIO = args.sampling_ratio

traces = [
    "Uber1", "Uber2", "Uber3",
    "IBMTrace004", "IBMTrace009", "IBMTrace011", "IBMTrace012", 
    "IBMTrace018", "IBMTrace027", "IBMTrace034", "IBMTrace045", 
    "IBMTrace055", "IBMTrace058", "IBMTrace066", "IBMTrace075",
    "IBMTrace080", "IBMTrace083", "IBMTrace096"
]
trace_to_minicache_mbs = {
    "Uber1": 16973, "Uber2": 16900, "Uber3": 16359,
    "IBMTrace004": 2955, "IBMTrace009": 33774, "IBMTrace011": 18914, "IBMTrace012": 26612,
    "IBMTrace018": 21667, "IBMTrace027": 20853, "IBMTrace034": 8460, "IBMTrace045": 946,
    "IBMTrace055": 66780, "IBMTrace058": 6727, "IBMTrace066": 90545, "IBMTrace075": 3749,
    "IBMTrace080": 18548, "IBMTrace083": 333140, "IBMTrace096": 411093
}
trace_to_keycounts = {
    "Uber1": 4000000, "Uber2": 4000000, "Uber3": 4000000,
    "IBMTrace004": 200000, "IBMTrace009": 2000000, "IBMTrace011": 1500000, "IBMTrace012": 1500000,
    "IBMTrace018": 65000000, "IBMTrace027": 1500000, "IBMTrace034": 2500000, "IBMTrace045": 15000000,
    "IBMTrace055": 6000000, "IBMTrace058": 3000000, "IBMTrace066": 5000000, "IBMTrace075": 2000000,
    "IBMTrace080": 1000000, "IBMTrace083": 25000000, "IBMTrace096": 30000000
}

trace_dirname = os.path.join(args.project_directory, 'traces')
tracedb_dirname = os.path.join(trace_dirname, 'tracedb')
os.makedirs(tracedb_dirname, exist_ok=True)

for idx, trace in enumerate(traces):
    tracefile_path, tracedb_path = os.path.join(trace_dirname, trace + '.csv'), os.path.join(tracedb_dirname, trace + '.db')
    output_dirpath = os.path.join(args.project_directory, 'scripts', 'miniature_simulation', trace)
    minicache_mb, keycount = trace_to_minicache_mbs[trace], trace_to_keycounts[trace]
    if os.path.exists(output_dirpath):
        print(f"Simulation results for {trace} already exist. Skipping...")
        continue    
    command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -f {tracefile_path} -p {tracedb_path} " + \
        f"-o {output_dirpath} -s {SAMPLING_RATIO} -u {minicache_mb} -k {keycount} -c {MINICACHE_COUNT} -e lru -m 15"
    print(command)
    os.system(command)
