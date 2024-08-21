package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

public class KetamaNodeKeyFormatter {
    /*
     * Helper functions that are used for Ketama hashing algorithm.
     * This class is mostly copied from https://github.com/couchbase/spymemcached/
     */
    public KetamaNodeKeyFormatter() {
    }

    public String getKeyForNode(String node, int repetition) {
        return node + "_" + repetition;
    }
}
