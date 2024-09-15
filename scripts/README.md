# Reproducing Evaluation Results

In this directory, we provide scripts for reproducing evaluation results in our paper. Before proceeding with the experiments, **ensure you have successfully built the project as detailed [here](../README.md)**.

We also recommend using a terminal multiplexer like [Screen](https://linuxize.com/post/how-to-use-linux-screen/), since each simulation run can take a long time. You can check the expected duration for each simulation [here](./expected_time.md).

## Table of Contents

- [Reproducing Evaluation Results](#reproducing-evaluation-results)
  - [Table of Contents](#table-of-contents)
  - [Traces and Pre-processed Data](#traces-and-pre-processed-data)
  - [Evaluations of Macaron’s Effectiveness in Minimizing Costs](#evaluations-of-macarons-effectiveness-in-minimizing-costs)
    - [Generating Figure 7](#generating-figure-7)
    - [Generating Figure 9](#generating-figure-9)
    - [Generating Figure 10](#generating-figure-10)
    - [Generating Figure 12](#generating-figure-12)
  - [Evaluations of Macaron with Cache Cluster](#evaluations-of-macaron-with-cache-cluster)
    - [Generating Figure 11](#generating-figure-11)
  - [Evaluations of Macaron's adaptivity](#evaluations-of-macarons-adaptivity)
    - [Generating Figure 8](#generating-figure-8)

## Traces and Pre-processed Data

We have uploaded the necessary traces to a [Google Drive folder](https://drive.google.com/drive/folders/1Rd78Rwo0LdLb64KSDdetSIT5QIL8s7Fp?usp=drive_link). Please download `MacaronTraces.tar.gz` (4.37 GB) and extract the traces into the `traces` directory (you need to create this directory) located within the root directory of your project. It is essential to place the data in the correct paths, as our provided scripts rely on these predetermined locations to run experiments.

```console
cd ${path/to/project_directory}
mkdir traces
cd traces
tar -xzf MacaronTraces.tar.gz
```

In the same Google Drive folder, we have provided the pre-processed data, which includes the results of all miniature simulations necessary for the main simulation.
This data is also reproducible, and you can find the instructions to regenerate them in [here](../scripts/miniature_simulation/README.md).
Please download `MacaronMiniatureSimulation.tar.gz` (22.4MB) and extract the files into the `${path/to/project_directory}/scripts/miniature_simulation` directory.

```console
cd ${path/to/project_directory}/scripts/miniature_simulation
tar -xzf MacaronMiniatureSimulation.tar.gz
```

The project is expected to be structured as follows:

```console
${path/to/project_directory}
│
├── traces/
│   ├── IBMTrace004.csv
│   ├── IBMTrace009.csv
│   └── ...
│
└── scripts/miniature_simulation/
    ├── IBMTrace004/
    ├── IBMTrace009/
    └── ...
```


## Evaluations of Macaron’s Effectiveness in Minimizing Costs

In this section, we conduct simulations with Macaron aimed at minimizing costs without a cache cluster.
The experiments presented here will generate the data for Figures 7, 9, and 12.

1. First, we need to create the necessary directories for running the simulation and storing the results for cross-cloud scenario.​

```console
python3 ${path/to/project_directory}/scripts/figure7/create_experiment_directories.py \
    -d ${path/to/project_directory} -c cross-cloud
```

2. To run simulations for all traces, use the following command.
If you prefer to run a specific baseline instead of all at once, specify the `method` parameter (`-m`) with one of the options: *remote*, *ecpc*, *macaron*, or *oracular*.
If you wish to run a simulation for a specific trace, set the trace name with `-t` parameter (a list of available trace names can be found [here](./expected_time.md)):

```console
python3 ${path/to/project_directory}/scripts/figure7/run_macaron_simulator.py \
    -d ${path/to/project_directory} -j ${path/to/simulator-runner.jar} -c cross-cloud
```

For example, if the project directory path is `/home/ubuntu/macaron_simulator`, you would use the following commands:

```console
python3 /home/ubuntu/macaron_simulator/scripts/figure7/create_experiment_directories.py \
    -d /home/ubuntu/macaron_simulator \
    -c cross-cloud

python3 /home/ubuntu/macaron_simulator/scripts/figure7/run_macaron_simulator.py \
    -d /home/ubuntu/macaron_simulator \
    -j /home/ubuntu/macaron_simulator/simulator/target/simulator-runner.jar \
    -c cross-cloud
```

We also offer an optional parameter `-s`, that limits the simulation to a subset of traces (Uber1, IBMTrace009, IBMTrace012), necessary for generating Figure 7.
However, to produce other figures, such as Figure 9, 10 and 12, the experiments must be executed across all traces.

### Generating Figure 7

3. You are now ready to generate Figure 7a. Execute the following command, and `figure7a.png` will be saved in the `figure7` directory:

```console
python3 ${path/to/project_directory}/scripts/figure7/figure7.py \
    -d ${path/to/project_directory} -c cross-cloud
```

To generate Figure 7b, repeat steps 1-3 as above, but replace `cross-cloud` with `cross-region`.
If you prefer not to run the entire simulation, you can directly download the precomputed data:

3a. Download the `costs-cross-region.tar.gz` from this [Google Drive folder](https://drive.google.com/drive/folders/1IggeA59ynxpwYK8ePfgsTw7-BXcDkBXh?usp=drive_link).  
3b. Untar the file in your project directory using the following command:
```console
tar -xzf costs-cross-region.tar.gz -C ${path/to/project_directory}/scripts/results/costs
```

The expected directory structure after untarring should be:
```console
${path/to/project_directory}/scripts/results/costs
│
├── cross-cloud/
└── cross-region/
```

### Generating Figure 9

4. The necessary data has already been generated from steps 1-2 above. To create and save `figure9.png` in the `figure9` directory, run the following command:

```console
python3 ${path/to/project_directory}/scripts/figure9/figure9.py \
    -d ${path/to/project_directory}
```

### Generating Figure 10

5. The necessary data has already been generated from steps 1-2 above. To create and save `figure10.png` in the `figure10` directory, run the following command:

```console
python3 ${path/to/project_directory}/scripts/figure10/figure10.py \
    -d ${path/to/project_directory}
```

### Generating Figure 12

6. To generate data for Figure 12a, repeat steps 1-2, but replace `cross-cloud` with `cross-10p` and `cross-1p`. Alternatively, download the precomputed results by following steps 3a-3b, replacing `costs-cross-10p.tar.gz` and `costs-cross-1p.tar.gz` instead. This will provide you with the necessary data to produce Figure 12a without needing to rerun simulations.

The expected directory structure after having data should be:
```console
${path/to/project_directory}/scripts/results/costs
│
├── cross-cloud/
├── cross-region/
├── cross-10p/
└── cross-1p/
```

Now, run the following command to save `figure12a.png` in the `figure12` directory:

```console
python3 ${path/to/project_directory}/scripts/figure12/figure12a.py \
    -d ${path/to/project_directory}
```

Execute the following command to save `figure12b.png` in the `figure12` directory:

```console
python3 ${path/to/project_directory}/scripts/figure12/figure12b.py \
    -d ${path/to/project_directory}
```

## Evaluations of Macaron with Cache Cluster

In this section, we conduct simulations of Macaron when the desired latencies are given as inputs.
The experiments will generate data for Figure 11.

7. First, create the necessary directories for running the simulation and storing the results:

```console
python3 ${path/to/project_directory}/scripts/figure11/create_experiment_directories.py \
    -d ${path/to/project_directory}
```

8. You can run all experiments at once or specify the `method` parameter (`-m`) with one of the options: *remote*, *replicated*, *ecpc*, *macaron*, or *macaron-cc*.
If you wish to run a simulation for a specific trace, set the trace name with `-t` parameter (a list of available trace names can be found [here](./expected_time.md)):

```console
python3 ${path/to/project_directory}/scripts/figure11/run_macaron_simulator.py \
    -d ${path/to/project_directory} -j ${path/to/simulator-runner.jar}
```

You can download the precomputed results [here](https://drive.google.com/drive/folders/1gECZ1vvmJ8tvoaVdA5q1gV6tpfQXKL8f?usp=drive_link). Extract the files under `${path/to/project_directory}/scripts/results` to maintain the expected directory structure:

```console
${path/to/project_directory}/scripts/results/latency/cross-cloud
│
├── ecpc
└── macaron
└── macaron-cc
└── remote
└── replicated
```

### Generating Figure 11

9. To generate the violin plot for Figure 11 in `figure11` directory, execute the following command:

```console
python3 ${path/to/project_directory}/scripts/figure11/figure11.py \
    -d ${path/to/project_directory}
```

* Important: The required version of Seaborn is 0.12.2. Please ensure compatibility by reinstalling Seaborn using the command: `pip3 install seaborn==0.12.2`, if your current version differs.

## Evaluations of Macaron's adaptivity

In this section, we evaluate the adaptivity of Macaron by running simulations on a composite trace formed by concatenating two distinct traces back-to-back. The experiments conducted here will generate data for Figure 8.

10. In this evaluation, we utilize the `cost-minisim-test.jar` file located in the `${path/to/project_directory}/simulator/target` directory to run the experiments. 
Execute the following command to generate Macaron’s optimization decisions for various decay factors:

```console
python3 ${path/to/project_directory}/scripts/figure8/run_cost_minisim_test.py \
    -d ${path/to/project_directory} -j {path/to/cost-minisim-test.jar}
```

Given the extensive time required to run all experiments, you can download the precomputed results [here](https://drive.google.com/drive/folders/1oFOoio1mySODrTCZj-Rj47jDO-2Kkjzx?usp=sharing). Extract the files under `${path/to/project_directory}/scripts/results` to maintain the expected directory structure:

```console
${path/to/project_directory}/scripts/results/adaptivity
│
├── IBMTrace_055_083/
└── IBMTrace_083_055/
```

### Generating Figure 8

11. To generate the adaptivity graph for Figure 8 in `figure8` directory, execute the following command:

```console
python3 ${path/to/project_directory}/scripts/figure8/figure8.py \
    -d ${path/to/project_directory}
```


