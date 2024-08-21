import argparse
import os

OPTS = "-Xmx100g -ea"
EXECCLASS = "edu.cmu.pdl.macaronsimulator.simulator.mrc.CostMiniatureSimulationTest"


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--project_directory', type=str, required=True, help='Project directory (absolute path)')
parser.add_argument('-j', '--jarfile_path', type=str, required=True, help='Path to the jar file')
parser.add_argument('-t', '--trace', type=str, help='Trace to evaluate')
args = parser.parse_args()

traces = ["IBMTrace_055_083", "IBMTrace_083_055"]
if args.trace is not None:
    assert args.trace in traces, f"Unknown trace: {args.trace}"
    traces = [args.trace]

for trace in traces:
    experiment_dirpath = os.path.join(args.project_directory, 'scripts', 'results', 'adaptivity', trace)
    miniature_simulation_dirpath = os.path.join(args.project_directory, 'scripts', 'miniature_simulation', trace)
    
    command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -c cross_cloud -m {miniature_simulation_dirpath} -o {experiment_dirpath}"
    print(command)
    os.system(command)
    
    command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -c cross_cloud -m {miniature_simulation_dirpath} -o {experiment_dirpath} -e -d 0.1"
    print(command)
    os.system(command)
    
    command = f"java {OPTS} -jar {args.jarfile_path} {EXECCLASS} -c cross_cloud -m {miniature_simulation_dirpath} -o {experiment_dirpath} -e -d 0.2"
    print(command)
    os.system(command)
    