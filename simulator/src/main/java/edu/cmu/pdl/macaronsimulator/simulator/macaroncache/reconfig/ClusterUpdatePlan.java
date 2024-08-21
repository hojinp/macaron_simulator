package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Pair;

/**
 * ClusterUpdatePlan class. This class is used to manage reconfiguration of the DRAMServer cluster.
 */
public class ClusterUpdatePlan {
    private List<Integer> stopNodes = new ArrayList<>(); // List of the indices of machines that will be stopped
    private int newNodeCount = 0; // Number of nodes that will be added to the cluster

    /**
     * Add a machine to the list of machines that will be stopped.
     * 
     * @param idx Index of the machine
     * @return The ClusterUpdatePlan object
     */
    public ClusterUpdatePlan addStopMachine(int idx) {
        stopNodes.add(idx);
        return this;
    }

    /**
     * Set the number of machines that will be added to the cluster.
     * 
     * @param cnt Number of machines
     * @return The ClusterUpdatePlan object
     */
    public ClusterUpdatePlan newMachineCount(int cnt) {
        newNodeCount = cnt;
        return this;
    }

    /**
     * Get the list of machines that will be stopped and the number of machines that will be added to the cluster.
     * 
     * @return A pair of the list of machines that will be stopped and the number of machines that will be added to the cluster
     */
    public Pair<List<Integer>, Integer> getPlan() {
        if (!stopNodes.isEmpty() && newNodeCount > 0)
            throw new RuntimeException("Invalid cluster update plan");
        return new Pair<>(stopNodes, newNodeCount);
    }
}
