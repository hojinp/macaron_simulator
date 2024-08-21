package edu.cmu.pdl.macaronsimulator.simulator.commons;

import java.util.List;

/**
 * Stores the information of the machines.
 */
public class MachineInfos {
    private List<MachineInfo> machines;

    public List<MachineInfo> getMachines() {
        return machines;
    }

    public void setMachines(List<MachineInfo> machines) {
        this.machines = machines;
    }
}
