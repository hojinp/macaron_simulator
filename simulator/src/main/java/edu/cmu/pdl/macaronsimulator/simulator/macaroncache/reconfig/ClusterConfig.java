package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig;

import java.util.ArrayList;
import java.util.List;

/**
 * ClusterConfig class. This class is used to manage reconfiguration of the MacServer cluster.
 */
public class ClusterConfig {
    static public int nodeIdx = 1; /* Next machine will receive this new machine index (e.g., CacheEngine-1, CacheEngine-2, ...) */
    static public List<String> cacheEngineNames = new ArrayList<>(); /* Name list of the cache engines */
    static public List<String> dramServerNames = new ArrayList<>(); /* Name list of the DRAM caches */
    static public List<String> prevCacheEngineNames = null; /* Prev. name list of the cache engines */
    static public List<String> prevDRAMServerNames = null; /* Prev. name list of the DRAM caches */
}
