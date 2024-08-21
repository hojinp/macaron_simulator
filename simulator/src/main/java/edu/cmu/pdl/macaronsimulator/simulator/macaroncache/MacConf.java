package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CachingPolicy;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.InclusionPolicy;

/**
 * MacaronCacheConfiguration class. Options can be parsed from the give configuration file. 
 */
public class MacConf {
    /* Caching policies */
    public static InclusionPolicy INCLUSION_POLICY = InclusionPolicy.INCLUSIVE;
    public static CachingPolicy CACHING_POLICY = CachingPolicy.WRITE_THROUGH;
    public static boolean PACKING = true;
    public static CacheType DRAM_CACHE_TYPE = CacheType.LRU;
    public static CacheType OSC_CACHE_TYPE = CacheType.LRU;

    /* Name of each components */
    public static String APP_NAME = null;
    public static String CONTROLLER_NAME = null;
    public static String OSC_NAME = null;
    public static String OSCM_NAME = null;
    public static List<String> ENGINE_NAME_LIST = new ArrayList<>();
    public static List<String> DRAM_NAME_LIST = new ArrayList<>();
    public static String DATALAKE_NAME = null;

    /* Number of cache nodes */
    public static int CACHE_NODE_COUNT = 0;
    public static String CACHE_NODE_MTYPE = null;

    /* Size of the default object storage cache size */
    public static long DEFAULT_OSC_CAPACITY = -1L;

    public static long MANUAL_DRAM_CAPACITY = -1L;

    public static long TTL = -1L;

    /**
     * Parse and set the configurations from the {@value #configFilename}.
     * 
     * @param configFilename filename that should be parsed
     */
    public static void setConfiguration(final String configFilename) {
        Yaml yaml = new Yaml();
        try (FileInputStream fis = new FileInputStream(configFilename)) {
            HashMap<String, Object> yamlMap = yaml.load(fis);
            for (final String key : yamlMap.keySet()) {
                Object val = yamlMap.get(key);
                switch (key) {
                    case "inclusionPolicy":
                        assert ((String) val).equals("INCLUSIVE");
                        INCLUSION_POLICY = InclusionPolicy.INCLUSIVE;
                        break;

                    case "cachingPolicy":
                        switch ((String) val) {
                            case "WRITE-THROUGH":
                                CACHING_POLICY = CachingPolicy.WRITE_THROUGH;
                                break;
                            default:
                                assert false;
                        }
                        break;

                    case "packing":
                        PACKING = (Boolean) val;
                        break;

                    case "cacheType":
                        CacheType cacheType = CacheType.valueOf((String) val);
                        if (!cacheType.isDRAMSupported()) {
                            try {
                                fis.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            throw new RuntimeException("Cache type " + cacheType.toString() + " is not supported");
                        }
                        DRAM_CACHE_TYPE = cacheType;
                        break;

                    case "ttl":
                        TTL = (long) ((int) val) * 60L * 1000L * 1000L; // convert minute to us
                        break;

                    case "oscCacheType":
                        cacheType = CacheType.valueOf((String) val);
                        if (!cacheType.isOSCSupported()) {
                            try {
                                fis.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            throw new RuntimeException("Cache type " + cacheType.toString() + " is not supported");
                        }
                        OSC_CACHE_TYPE = cacheType;
                        break;

                    case "cacheNodeCount":
                        CACHE_NODE_COUNT = (int) val;
                        break;

                    case "cacheNodeMachineType":
                        CACHE_NODE_MTYPE = (String) val;
                        break;

                    case "manualDRAMCapacityMB": // in MB
                        MANUAL_DRAM_CAPACITY = (long) ((int) val) * 1024L * 1024L;
                        break;
                        
                    case "oscCapacity": // in GB
                        DEFAULT_OSC_CAPACITY = (long) ((int) val) * 1024L * 1024L * 1024L;
                        break;

                    default:
                        assert false;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Print the configurations.
     */
    public static void printConfigs() {
        Logger.getGlobal().info("Macaron cache configurations");
        Logger.getGlobal().info("* Inclusion policy: " + INCLUSION_POLICY.toString());
        Logger.getGlobal().info("* Caching policy: " + CACHING_POLICY.toString());
        Logger.getGlobal().info("* DRAM cache policy: " + DRAM_CACHE_TYPE.toString());
        Logger.getGlobal().info("* OSC cache policy: " + OSC_CACHE_TYPE.toString());
        Logger.getGlobal().info("* Cache node count: " + CACHE_NODE_COUNT);
        Logger.getGlobal().info("* Cache node machine type: " + CACHE_NODE_MTYPE);
        Logger.getGlobal().info("* OSC capcity: " + String.valueOf(DEFAULT_OSC_CAPACITY));
    }
}
