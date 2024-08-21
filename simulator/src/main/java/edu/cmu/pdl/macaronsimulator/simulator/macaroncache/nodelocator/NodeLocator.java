package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;

/**
 * Implement a corresponding hash function to implement NodeLocator.
 */
public interface NodeLocator {
    String getNode(final ObjContent obj);

    String getNode(final int objId);

    String getNode();

    String getType();
}
