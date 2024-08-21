package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;

public class FixedNodeLocator implements NodeLocator {
    final String targetName;

    public FixedNodeLocator(String targetName) {
        this.targetName = targetName;
    }

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
        return "Fixed";
    }
}
