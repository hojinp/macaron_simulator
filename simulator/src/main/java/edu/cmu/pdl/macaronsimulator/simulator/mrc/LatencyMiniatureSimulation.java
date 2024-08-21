package edu.cmu.pdl.macaronsimulator.simulator.mrc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.AccessInfo;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim.LatencyLRUCache;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class LatencyMiniatureSimulation {
    private static int HASH_SIZE = (int) Math.pow(10, 8);
    private static long MB_TO_BYTE = 1024L * 1024L;

    public static boolean CONSIDER_TIMING = true;

    // object id that is out of the sampled hash space will be ignored
    private int miniCacheCount = 0;
    private int cacheSizeUnitMB = 0;
    private int sampledHashSpace; // any object id that is out of the sampled hash space will be ignored
    private int[] objectIdToSampledId; // use smaller sample id space for memory efficiency
    private int sampleKeyMaxId = 0; // maximum sampled key id

    private Map<Integer, LatencyLRUCache> dramCaches = null;
    private Map<Integer, LatencyLRUCache> oscCaches = null;
    private Map<Integer, List<Long>> latencyLists = null;
    private LatencyGenerator lg = null;

    private final String latency_filename;
    private final String reqcount_filename;

    public LatencyMiniatureSimulation(double samplingRatio, int miniCacheCount, int cacheSizeUnitMB, int maxObjectId,
            long oscSizeByte) {
        Logger.getGlobal().info("Start initializing latency miniature simulation");
        this.miniCacheCount = miniCacheCount;
        this.sampledHashSpace = (int) (HASH_SIZE * samplingRatio);
        this.cacheSizeUnitMB = cacheSizeUnitMB;
        this.objectIdToSampledId = new int[maxObjectId + 1];
        for (int i = 0; i < objectIdToSampledId.length; i++) {
            if (hashId(i) > sampledHashSpace) // spatial sampling
                continue;
            objectIdToSampledId[i] = sampleKeyMaxId++;
        }

        Logger.getGlobal().info("Start preparing imcCaches and oscCaches");
        this.dramCaches = new HashMap<>(); // key: cache size (MB), value: miniature caches
        this.oscCaches = new HashMap<>(); // key: cache size (MB), value: miniature caches
        this.latencyLists = new HashMap<>();
        for (int i = 0; i < miniCacheCount; i++) {
            int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
            long cacheSizeByte = (long) cacheSizeMB * MB_TO_BYTE;
            dramCaches.put(cacheSizeMB, new LatencyLRUCache((long) (cacheSizeByte * samplingRatio), sampleKeyMaxId));
            oscCaches.put(cacheSizeMB, new LatencyLRUCache((long) (oscSizeByte * samplingRatio), sampleKeyMaxId));
            latencyLists.put(cacheSizeMB, new ArrayList<>());
        }
        Logger.getGlobal().info("Done preparing imcCaches and oscCaches");

        lg = new LatencyGenerator();

        latency_filename = Paths.get(ProfileInfoStore.LOG_DIRNAME, "latency_minisim.csv").toString();
        reqcount_filename = Paths.get(ProfileInfoStore.LOG_DIRNAME, "reqcount_minisim.csv").toString();
        writeLineToFile("Time(min),CacheSize,Latency(us)\n", latency_filename, true);
        writeLineToFile("Time(min),RequestCount,SampledGetCount\n", reqcount_filename, true);

        Logger.getGlobal().info("Done initializing latency miniature simulation");
    }

    /**
     * Get the cache sizes of the miniature caches.
     * 
     * @return cache sizes of the miniature caches
     */
    public int[] getCacheSizes() {
        int[] cacheSizes = new int[miniCacheCount];
        for (int i = 0; i < miniCacheCount; i++) {
            cacheSizes[i] = cacheSizeUnitMB * (i + 1);
        }
        return cacheSizes;
    }

    /**
     * Run miniature caches with the given timestamp. This function will return the average latency of each cache size.
     * 
     * @param ts timestamp when this function is called
     * @return average latency of each cache size
     */
    public Map<Integer, Long> run(long ts) {
        Logger.getGlobal().info("Start running miniature caches");
        long startTime = System.currentTimeMillis();

        int reqCnt = TraceDataDB.totalAccessCount, sampleReqCnt = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Integer>> futures = new ArrayList<>();
        for (int cacheSizeMB : dramCaches.keySet()) {
            final LatencyLRUCache dramCache = dramCaches.get(cacheSizeMB);
            final LatencyLRUCache oscCache = oscCaches.get(cacheSizeMB);
            final List<Long> latencyList = latencyLists.get(cacheSizeMB);
            latencyList.clear();
            Future<Integer> future = executorService.submit(() -> {
                int sampledGetReqCount = 0;
                for (int idx = 0; idx < TraceDataDB.totalAccessCount; idx++) {
                    AccessInfo access = TraceDataDB.accessHistory[idx];
                    if (hashId(access.objectId) > sampledHashSpace) // spatial sampling
                        continue;
                    int sampledId = objectIdToSampledId[access.objectId];
                    switch (access.qType) {
                        case PUT:
                            dramCache.put(sampledId, access.val, access.ts);
                            oscCache.put(sampledId, access.val, access.ts);
                            break;

                        case GET:
                            sampledGetReqCount++;
                            long dramLat = lg.getE2ELatency(CompType.DRAM, access.val);
                            long oscLat = lg.getE2ELatency(CompType.OSC, access.val);
                            long dlLat = lg.getE2ELatency(CompType.DATALAKE, access.val);
                            long dramBirth = dramCache.getBirth(sampledId);
                            long oscBirth = oscCache.getBirth(sampledId);

                            if (CONSIDER_TIMING) {
                                if (dramBirth == -1L) { // miss from DRAM
                                    if (oscBirth == -1L) { // miss from DRAM and OSC: wait for retrieving data from DL
                                        latencyList.add(dlLat);
                                    } else {
                                        if (oscBirth + dlLat <= access.ts) { // miss from DRAM and hit from OSC (already in OSC)
                                            latencyList.add(oscLat);
                                        } else { // miss from IMC, hit from OSC, but it's still waiting for retrieving data from DL
                                            latencyList.add(dlLat - (access.ts - oscBirth));
                                        }
                                    }
                                } else { // hit from DRAM
                                    if (oscBirth == -1L) {// hit from DRAM, but miss from OSC
                                        if (dramBirth + dlLat <= access.ts) { // hit from DRAM
                                            latencyList.add(dramLat);
                                        } else { // hit from DRAM, but still wait for retrieving data from DL
                                            latencyList.add(dlLat - (access.ts - dramBirth));
                                        }
                                    }

                                    if (oscBirth + dlLat <= access.ts) { // hit from DRAM and OSC (already in OSC)
                                        if (dramBirth + oscLat <= access.ts) { // hit from DRAM
                                            latencyList.add(dramLat);
                                        } else { // miss from DRAM and hit from OSC
                                            latencyList.add(oscLat);
                                        }
                                    } else { // somehow, hit from DRAM, but OSC is still waiting for retrieving data from DL
                                        if (dramBirth + dlLat <= access.ts) { // hit from DRAM
                                            latencyList.add(dramLat);
                                        } else { // hit from DRAM, but still wait for retrieving data from DL
                                            latencyList.add(dlLat - (access.ts - dramBirth));
                                        }
                                    }
                                }
                            } else {
                                if (dramBirth != -1L) {
                                    latencyList.add(dramLat);
                                } else if (oscBirth != -1L) {
                                    latencyList.add(oscLat);
                                } else {
                                    latencyList.add(dlLat);
                                }
                            }

                            if (dramBirth == -1L)
                                dramCache.put(sampledId, access.val, access.ts);
                            else
                                dramCache.get(sampledId, access.ts);

                            if (oscBirth == -1L)
                                oscCache.put(sampledId, access.val, access.ts);
                            else
                                oscCache.get(sampledId, access.ts);
                            break;

                        case DELETE:
                            dramCache.delete(sampledId);
                            oscCache.delete(sampledId);
                            break;

                        default:
                            throw new RuntimeException("Cannot reach here");
                    }
                }
                return sampledGetReqCount;
            });
            futures.add(future);
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS))
                executorService.shutdownNow();
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        try {
            sampleReqCnt = futures.get(0).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        long elpasedTime = System.currentTimeMillis() - startTime;
        Logger.getGlobal().info("Done running miniature caches, elapsed time: " + String.valueOf(elpasedTime) + "ms");

        int curMin = (int) (ts / 60. / 1000000.); /* conver us to min */
        writeLineToFile(String.format("%d,%d,%d\n", curMin, reqCnt, sampleReqCnt), reqcount_filename, false);

        if (latencyLists.get(cacheSizeUnitMB).isEmpty()) { // no latency data in this phase
            for (int i = 0; i < miniCacheCount; i++) {
                int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
                String newLine = String.format("%d,%d,%d\n", curMin, cacheSizeMB, 0);
                writeLineToFile(newLine, latency_filename, false);
            }
            return null;
        }

        Map<Integer, Long> latencies = new HashMap<>();
        for (int i = 0; i < miniCacheCount; i++) {
            int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
            OptionalDouble avgLat = latencyLists.get(cacheSizeMB).stream().mapToLong(Long::longValue).average();
            latencies.put(cacheSizeMB, (long) avgLat.getAsDouble());
        }

        for (int i = 0; i < miniCacheCount; i++) {
            int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
            String newLine = String.format("%d,%d,%d\n", curMin, cacheSizeMB, latencies.get(cacheSizeMB));
            writeLineToFile(newLine, latency_filename, false);
        }

        return latencies;
    }

    /**
     * Update the cache size of the miniature caches.
     * 
     * @param oscCacheSizeMB new cache size of the miniature caches
     */
    public void updateOSCSize(int oscCacheSizeMB) {
        for (LatencyLRUCache cache : oscCaches.values())
            cache.updateCacheSize(oscCacheSizeMB * MB_TO_BYTE);
    }

    /**
     * Get the hash id of the given object id.
     * 
     * @param n object id
     * @return hash id
     */
    private int hashId(int n) {
        String digested = DigestUtils.sha1Hex(String.valueOf(n)).substring(0, 15);
        return (int) (Long.parseLong(digested, 16) % HASH_SIZE);
    }

    public void close() {
    }

    /**
     * Write the given line to the given file.
     * 
     * @param line line to write
     * @param filename file to write
     * @param isNewFile if true, create a new file
     */
    private void writeLineToFile(String line, String filename, boolean isNewFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, !isNewFile))) {
            writer.write(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
