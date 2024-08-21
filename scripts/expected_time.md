# Expected Time for Each Simulation

We conducted simulations on a machine with the following specifications: Cray/Appro GB512X with 32 Threads Xeon E5-2670 @ 2.60GHz and 128 GiB DDR3 RAM.
The table below details the runtime for each simulation performed on this machine.

| Simulation                                                | Trace             | Elapsed time (hr) |
| :-------------------------                                |:------------------|:------------------|
| Macaron w/o Cache Cluster <br>(For each Cross-X scenario) |Total              | 21.7              |
|                                                           |IBMTrace004        | 0.4               |
|                                                           |IBMTrace009        | 0.4               |
|                                                           |IBMTrace011        | 0.5               |
|                                                           |IBMTrace012        | 1.3               |
|                                                           |IBMTrace018        | 1.7               |
|                                                           |IBMTrace027        | 0.4               |
|                                                           |IBMTrace034        | 0.6               |
|                                                           |IBMTrace045        | 0.6               |
|                                                           |IBMTrace055        | 0.6               |
|                                                           |IBMTrace058        | 0.5               |
|                                                           |IBMTrace066        | 0.5               |
|                                                           |IBMTrace075        | 0.5               |
|                                                           |IBMTrace080        | 1.2               |
|                                                           |IBMTrace083        | 1.1               |
|                                                           |IBMTrace096        | 0.8               |
|                                                           |Uber1              | 3.5               |
|                                                           |Uber2              | 3.6               |
|                                                           |Uber3              | 3.5               |
| Macaron w/ Cache Cluster                                  |Total              | 2.5               |
|                                                           |IBMTrace009        | 0.7               |
|                                                           |IBMTrace011        | 0.7               |
|                                                           |IBMTrace055        | 1.1               |
| Macaron's Adaptivity                                      |Total              | 19.4              |
|                                                           |IBMTrace_055_083   | 9.8               |
|                                                           |IBMTrace_083_055   | 9.6               |
