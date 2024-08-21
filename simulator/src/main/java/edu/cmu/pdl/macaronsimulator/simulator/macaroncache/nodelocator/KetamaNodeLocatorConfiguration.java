package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

public class KetamaNodeLocatorConfiguration {
    /*
     * Configuration of the Ketama consistent hashing.
     * This class is mostly copied from https://github.com/couchbase/spymemcached/
     */
    private final int numReps = 28;
    private final KetamaNodeKeyFormatter ketamaNodeKeyFormatter;

    public KetamaNodeLocatorConfiguration() {
        ketamaNodeKeyFormatter = new KetamaNodeKeyFormatter();
    }

    public int getNodeRepetitions() {
        return numReps;
    }

    public String getKeyForNode(String node, int repetition) {
        return ketamaNodeKeyFormatter.getKeyForNode(node, repetition);
    }
}
