# Reproducing Miniature Simulation Results

In this directory, we provide a script to reproduce the miniature simulation results used for the main simulation.

In this simulation, we utilize the `cost-minisim-runner.jar` file located in the `${path/to/project_directory}/simulator/target` directory to run the experiments.

The following command will execute miniature simulation experiments for all traces with 200 mini-caches and sampling ratio of 0.05, which are the default settings used in our paper. You can adjust the number of mini-caches and the sampling ratio with the `minicache_count` (`-m`) and `sampling_ratio` (`-s`) parameters, respectively.

```console
python3 ${path/to/project_directory}/scripts/miniature_simulation/run_miniature_simulation.py \
    -d ${path/to/project_directory} -j ${path/to/cost-minisim-runner.jar}
```

The results will be stored in the `${path/to/project_directory}/scripts/miniature_simulation` directory.