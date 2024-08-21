package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;

/**
 * SameMachineNodeLocator assumes that proxy server and IMC server are co-located in a single machine and 1-1 mapping
 * to each other.
 */
public class SameMachineNodeLocator implements NodeLocator {
    final String targetName;

    public SameMachineNodeLocator(final String componentName) {
        if (componentName.startsWith("DRAM") || componentName.startsWith("CACHE_ENGINE")) {
            String[] splits = componentName.split("-", 2);
            targetName = (componentName.startsWith("DRAM") ? "CACHE_ENGINE-" : "DRAM-") + splits[1];
        } else {
            throw new RuntimeException("Unknown componentName prefix: " + componentName);
        }
    }

    /**
     * Return the correct node name corresponding to the given key value
     */
    @Override
    public String getNode(final ObjContent obj) {
        return targetName;
    }

    @Override
    public String getNode(final int objId) {
        return targetName;
    }

    @Override
    public String getNode() {
        return targetName;
    }

    @Override
    public String getType() {
        return "SameMachine";
    }
}
