# Reducing cross-cloud/region costs with the auto-configuring MACARON cache

This repository contains the artifacts needed to reproduce the results presented in our SOSP '24 paper titled "Reducing Cross-Cloud/Region Costs with the Auto-Configuring MACARON Cache."

## Table of Contents
- [Reducing cross-cloud/region costs with the auto-configuring MACARON cache](#reducing-cross-cloudregion-costs-with-the-auto-configuring-macaron-cache)
  - [Table of Contents](#table-of-contents)
  - [Environment Setup](#environment-setup)
  - [Source Code](#source-code)
  - [Setup Instructions](#setup-instructions)
  - [Reproducing Evaluation Results](#reproducing-evaluation-results)

## Environment Setup
These instructions have been validated on **Ubuntu 22.04 LTS**. Some modifications may be necessary for other Linux distributions.

It is strongly recommended to use a machine with at least 120 GB of memory to prevent out-of-memory issues with some large traces.

## Source Code

The source code for the Macaron Simulator is available at:
```console
git clone git@github.com:hojinp/macaron_simulator.git
```

The project is organized into the following directories:
- `common/`, `simulator/`: Contains the implementation of the Macaron Simulator.
- `oracular/`: Contains the implementation of the oracular solution in Python.
- `scripts/`: Contains Python scripts that run simulations, reproduce results presented in the paper, and generate graphs.

## Setup Instructions

1. Install OpenJDK 17
```console
sudo apt update
sudo apt install openjdk-17-jre
sudo apt install openjdk-17-jdk
java --version
```

2. Install Apache Maven 3.6.3

Download Maven from the official source:
```console
wget https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip
unzip apache-maven-3.6.3
```

Add the Maven `bin` directory to your `PATH` in your `.bashrc` file:
```console
export PATH="${path/to/maven_directory}/bin:$PATH"
```

Verify the installation:
```console
mvn --version
```

3. Build the Macaron Simulator

Navigate to the project directory and compile:
```console
cd ${path/to/project_directory}
mvn clean package 
```

This command creates a `jar` file under `${path/to/project_directory}/simulator/target/simulator-runner.jar`, which is required for running the simulation scripts.

4. To run the Python scripts that generate the graphs, please install the necessary libraries using the following command:

```console
pip3 install matplotlib pandas seaborn==0.12.2
```

## Reproducing Evaluation Results

Scripts are provided in the `./scripts` directory to facilitate the reproduction of our evaluation results. Follow the detailed instructions provided in the [scripts directory](./scripts/README.md).
