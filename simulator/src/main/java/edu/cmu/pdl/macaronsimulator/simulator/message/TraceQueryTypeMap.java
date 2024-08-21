package edu.cmu.pdl.macaronsimulator.simulator.message;

import java.util.HashMap;
import java.util.Map;

/**
 * TraceQueryTypeMap class.
 * The trace should have the following mapping between numbers and request types, and this mapping will be used to 
 * parse the trace files.
 * 
 * Mapping information:
 * - 0: PUT operation
 * - 1: GET operation
 * - 2: DELETE operation
 */
public class TraceQueryTypeMap {
    private static final Map<Integer, QType> idxToQType = new HashMap<>();

    static {
        idxToQType.put(0, QType.PUT);
        idxToQType.put(1, QType.GET);
        idxToQType.put(2, QType.DELETE);
    }

    /**
     * Check if the given index is valid.
     * @param idx index
     * @return true if the index is valid, false otherwise
     */
    public static boolean valid(final int idx) {
        return idxToQType.containsKey(idx);
    }

    /**
     * Get Query type from the given index.
     * @param idx index
     * @return query type
     */
    public static QType get(final int idx) {
        return idxToQType.get(idx);
    }
}
