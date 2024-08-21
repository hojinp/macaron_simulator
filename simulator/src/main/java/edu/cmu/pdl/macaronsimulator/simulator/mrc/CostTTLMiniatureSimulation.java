package edu.cmu.pdl.macaronsimulator.simulator.mrc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.math3.util.Pair;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.AccessInfo;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim.CostTTLCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class CostTTLMiniatureSimulation {

    private static int HASH_SIZE = (int) Math.pow(10, 8);

    private double samplingRatio;
    private int sampledHashSpace;
    private int miniCacheCount = 0;

    private Map<Integer, CostTTLCache> caches = null;
    private int[] oidToNid;

    private List<Integer> minuteList = new ArrayList<>();
    private List<double[]> missRatiosList = new ArrayList<>();
    private List<long[]> bytesMissList = new ArrayList<>();
    private List<long[]> cacheSizeList = new ArrayList<>();
    private List<Integer> sampledRequestsList = new ArrayList<>();
    private int totalReqCnt = 0, totalSampleReqCnt = 0;

    // Log file names
    private String mrcFilename = null;
    private String reqCntFilename = null;
    private String expectedMRCFilename = null;
    private String expectedBMCFilename = null;
    private String expectedCCFilename = null;

    public CostTTLMiniatureSimulation(String miniSimDirname, double samplingRatio, int miniCacheCount,
            CacheType cacheType, int maxObjectId) {
        this.samplingRatio = samplingRatio;
        this.miniCacheCount = miniCacheCount;
        assert miniCacheCount > 2; // make sure TTL=1hr and 6hr are supported
        this.sampledHashSpace = (int) (HASH_SIZE * samplingRatio);
        if (cacheType != CacheType.TTL)
            throw new RuntimeException("Unexpected cache type: " + cacheType);

        int arrayLength = maxObjectId + 1;
        this.oidToNid = new int[arrayLength];

        // Assign new object id to the objects that will be sampled (for memory efficiency)
        int newKeyMaxId = 0;
        for (int i = 0; i < arrayLength; i++) {
            if (hashId(i) > sampledHashSpace) // spatial sampling
                continue;
            oidToNid[i] = newKeyMaxId++;
        }

        // Initialize miniature caches
        Logger.getGlobal().info("Initializing miniature caches");
        this.caches = new HashMap<>(); // key: cache size (MB), value: miniature caches
        for (int i = 0; i < miniCacheCount; i++) {
            long ttl = (i == 0) ? 1L : (i == 1) ? 6L : 12L * (i - 1);
            caches.put((int) ttl, new CostTTLCache(ttl * 60L * 60L * 1000L * 1000L, newKeyMaxId));
        }
        Logger.getGlobal().info("Finished initializing miniature caches");

        // Logging related variables
        if (!Files.isDirectory(Paths.get(miniSimDirname)))
            throw new RuntimeException("Directory " + miniSimDirname + " does not exist");
        this.mrcFilename = Paths.get(miniSimDirname, "mrc.csv").toString();
        this.reqCntFilename = Paths.get(miniSimDirname, "req-count.csv").toString();
        writeToFile("Time(min),TTL(hr),MissRatio,BytesMiss,CacheSize\n", mrcFilename, true);
        writeToFile("Time(min),TotalReqCnt,TotalSampleReqCnt,LocalReqCnt,LocalSampleReqCnt\n", reqCntFilename, true);
    }

    public CostTTLMiniatureSimulation(String miniSimDirname) {
        if (!Files.isDirectory(Paths.get(miniSimDirname)))
            throw new RuntimeException("Directory " + miniSimDirname + " does not exist");

        this.mrcFilename = Paths.get(miniSimDirname, "mrc.csv").toString();
        this.reqCntFilename = Paths.get(miniSimDirname, "req-count.csv").toString();

        // Cache sizes are multiplication of the minimum cache size among the values logged in the file. We can get the
        // number of mini-caches by dividing the maximum cache size by the minimum cache size.
        String line = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(mrcFilename))) {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Time"))
                    continue;
                String[] splits = line.split(",");
                int ttl = Integer.parseInt(splits[1].strip());
                miniCacheCount = Math.max(miniCacheCount, ttl / 12 + 2);
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }

        // Read MRC file and load the pre-computed MRC results
        try (BufferedReader reader = new BufferedReader(new FileReader(mrcFilename))) {
            double[] missRatios = null;
            long[] bytesMiss = null;
            long[] cacheSizes = null;
            int curMin = -1;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Time"))
                    continue;

                String[] splits = line.split(",");
                int minute = Integer.parseInt(splits[0].strip());

                if (minute != curMin) {
                    if (missRatios != null) {
                        minuteList.add(curMin);
                        missRatiosList.add(missRatios);
                        bytesMissList.add(bytesMiss);
                        cacheSizeList.add(cacheSizes);
                    }
                    curMin = minute;
                    missRatios = new double[miniCacheCount];
                    bytesMiss = new long[miniCacheCount];
                    cacheSizes = new long[miniCacheCount];
                }

                int ttl = Integer.parseInt(splits[1].strip());
                int idx = ttl == 1 ? 0 : ttl == 6 ? 1 : ttl / 12 + 1;
                if (idx < 0 || idx >= miniCacheCount)
                    throw new RuntimeException("Unexpected TTL: " + ttl);

                missRatios[idx] = Double.parseDouble(splits[2].strip());
                bytesMiss[idx] = Long.parseLong(splits[3].strip());
                cacheSizes[idx] = Long.parseLong(splits[4].strip());
            }

            if (missRatios != null) {
                minuteList.add(curMin);
                missRatiosList.add(missRatios);
                bytesMissList.add(bytesMiss);
                cacheSizeList.add(cacheSizes);
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }

        // Read request-count file
        try (BufferedReader reader = new BufferedReader(new FileReader(reqCntFilename))) {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Time"))
                    continue;
                String[] splits = line.split(",");
                sampledRequestsList.add(Integer.parseInt(splits[4].strip()));
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }

        int size = minuteList.size();
        if (size != missRatiosList.size() || size != bytesMissList.size() || size != sampledRequestsList.size())
            throw new RuntimeException("Inconsistent data size: " + size);

        expectedMRCFilename = Paths.get(miniSimDirname, String.format("expected-mrc.csv", samplingRatio)).toString();
        expectedBMCFilename = Paths.get(miniSimDirname, String.format("expected-bmc.csv", samplingRatio)).toString();
        expectedCCFilename = Paths.get(miniSimDirname, String.format("expected-cc.csv", samplingRatio)).toString();
        writeToFile("Time(min),TTL(hr),ExpectedMissRatio\n", expectedMRCFilename, true);
        writeToFile("Time(min),TTL(hr),ExpectedBytesMiss\n", expectedBMCFilename, true);
        writeToFile("Time(min),TTL(hr),ExpectedCacheSize\n", expectedCCFilename, true);
    }

    public int[] getTTLHRs() {
        int[] ttlHRs = new int[miniCacheCount];
        for (int i = 0; i < miniCacheCount; i++)
            ttlHRs[i] = (i == 0) ? 1 : (i == 1) ? 6 : 12 * (i - 1);
        return ttlHRs;
    }

    /**
     * Expected MRC for the next time window
     * 
     * @param curMin the time when MRC result is returned
     * @return moving average MRC result
     */
    public double[] getMRC(int curMin) {
        int statWindowMin = ReconfigEvent.getStatWindowMin(), begMin = 0;
        int timeWindowCnts = curMin - begMin < statWindowMin ? 1 : (curMin - begMin) / statWindowMin;
        if (curMin < begMin || timeWindowCnts < 1)
            throw new RuntimeException("Cannot get MRC result before " + begMin + " min or with <1 time window");

        int localCnts[] = new int[timeWindowCnts];
        for (int i = 0; i < timeWindowCnts; i++) {
            int startMin = curMin - (i + 1) * statWindowMin, endMin = curMin - i * statWindowMin;
            for (int j = 0; j < minuteList.size(); j++) {
                int minute = minuteList.get(j);
                if (startMin < minute && minute <= endMin)
                    localCnts[i] += sampledRequestsList.get(j);
            }
        }
        double coefficients[] = ReconfigEvent.getWeightDecayCoefficient(localCnts);
        if (coefficients == null)
            return null;

        double mrc[] = new double[miniCacheCount];
        for (int i = 0; i < timeWindowCnts; i++) {
            int startMin = curMin - (i + 1) * statWindowMin, endMin = curMin - i * statWindowMin;
            for (int j = 0; j < minuteList.size(); j++) {
                int minute = minuteList.get(j);
                int misses[] = new int[miniCacheCount];
                if (startMin < minute && minute <= endMin) {
                    double[] mrs = missRatiosList.get(j);
                    for (int k = 0; k < miniCacheCount; k++)
                        misses[k] += sampledRequestsList.get(j) * mrs[k];
                }
                for (int k = 0; k < miniCacheCount; k++)
                    if (localCnts[i] != 0)
                        mrc[k] += coefficients[i] * misses[k] / localCnts[i];
            }
        }

        for (int i = 0; i < miniCacheCount; i++) {
            int ttl = (i == 0) ? 1 : (i == 1) ? 6 : 12 * (i - 1);
            writeToFile(String.format("%d,%d,%.6f\n", curMin, ttl, mrc[i]), expectedMRCFilename, false);
        }

        return mrc;
    }

    /**
     * Expected BMC for the next time window
     * 
     * @param curMin the time when BMC result is returned
     * @return moving average BMC result
     */
    public long[] getBMC(int curMin) {
        int statWindowMin = ReconfigEvent.getStatWindowMin(), begMin = 0;
        int timeWindowCnts = curMin - begMin < statWindowMin ? 1 : (curMin - begMin) / statWindowMin;
        if (curMin < begMin || timeWindowCnts < 1)
            throw new RuntimeException("Cannot get MRC result before " + begMin + " min or with <1 time window");

        int localCnts[] = new int[timeWindowCnts];
        for (int i = 0; i < timeWindowCnts; i++) {
            localCnts[i] = 1;
        }
        double coefficients[] = ReconfigEvent.getWeightDecayCoefficient(localCnts);

        long[] sumBytesMiss = new long[miniCacheCount];
        for (int i = 0; i < timeWindowCnts; i++) {
            int startMin = curMin - (i + 1) * statWindowMin, endMin = curMin - i * statWindowMin;
            for (int j = 0; j < minuteList.size(); j++) {
                int minute = minuteList.get(j);
                long bytesMisses[] = new long[miniCacheCount];
                if (startMin < minute && minute <= endMin) {
                    long[] bytesMiss = bytesMissList.get(j);
                    for (int k = 0; k < miniCacheCount; k++)
                        bytesMisses[k] += bytesMiss[k];
                }
                for (int k = 0; k < miniCacheCount; k++)
                    sumBytesMiss[k] += coefficients[i] * bytesMisses[k];
            }
        }

        for (int i = 0; i < miniCacheCount; i++) {
            int ttl = (i == 0) ? 1 : (i == 1) ? 6 : 12 * (i - 1);
            writeToFile(String.format("%d,%d,%s\n", curMin, ttl, sumBytesMiss[i]), expectedBMCFilename, false);
        }

        return sumBytesMiss;
    }

    /**
     * Expected Cache Size for the next time window
     * 
     * @param curMin the time when Cache Size result is returned
     * @return expected Cache Size
     */
    public long[] getCSC(int curMin) {
        int statWindowMin = ReconfigEvent.getStatWindowMin(), begMin = 0;
        int timeWindowCnts = curMin - begMin < statWindowMin ? 1 : Math.min((curMin - begMin) / statWindowMin, 24 * 60 / statWindowMin);
        if (curMin < begMin || timeWindowCnts < 1)
            throw new RuntimeException("Cannot get MRC result before " + begMin + " min or with <1 time window");

        long[] csc = new long[miniCacheCount];
        boolean found = false;
        for (int i = 0; i < timeWindowCnts && !found; i++) {
            int startMin = curMin - (i + 1) * statWindowMin, endMin = curMin - i * statWindowMin;
            for (int j = minuteList.size() - 1; j >= 0; j--) {
                int minute = minuteList.get(j);
                long[] cacheSize = cacheSizeList.get(j);
                if (startMin < minute && minute <= endMin) {
                    for (int k = 0; k < miniCacheCount; k++)
                        csc[k] += cacheSize[k];
                    found = true;
                    break;
                }
            }
        }

        for (int i = 0; i < miniCacheCount; i++) {
            int ttl = (i == 0) ? 1 : (i == 1) ? 6 : 12 * (i - 1);
            writeToFile(String.format("%d,%d,%s\n", curMin, ttl, csc[i]), expectedCCFilename, false);
        }

        return csc;
    }

    /**
     * Return the minute list
     * 
     * @return the minute list
     */
    List<Integer> getMinuteList() {
        return minuteList;
    }

    /**
     * Return the number of sampled requests at the given minute
     * 
     * @return the number of sampled requests at the given minute
     */
    int getSampledRequestCount(int minute) {
        for (int i = 0; i < minuteList.size(); i++) {
            if (minuteList.get(i) == minute)
                return sampledRequestsList.get(i);
        }
        return -1;
    }

    /**
     * Run a miniature simulation.
     * 
     * @param ts the time (in microsecond) to run the simulation
     */
    public void run(long ts) {
        Logger.getGlobal().info("Running MiniatureSimulation for " + (ts / 1000000 / 60) + " min");

        int reqCnt = TraceDataDB.totalAccessCount, sampleReqCnt = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Integer>> futures = new ArrayList<>();
        for (final int ttl : caches.keySet()) {
            Future<Integer> future = executorService.submit(() -> {
                final CostTTLCache cache = caches.get(ttl);
                int tSampleReqCnt = 0;

                for (int idx = 0; idx < reqCnt; idx++) {
                    AccessInfo access = TraceDataDB.accessHistory[idx];
                    if (hashId(access.objectId) > sampledHashSpace) // spatial sampling
                        continue;

                    int nid = oidToNid[access.objectId];
                    switch (access.qType) {
                        case PUT:
                            cache.putWithoutEviction(nid, access.val, access.ts);
                            break;

                        case GET:
                            tSampleReqCnt++;
                            long size = cache.get(nid, access.ts);
                            if (size == 0L) {
                                cache.putWithoutEviction(nid, access.val, access.ts);
                                cache.addBytesMiss(access.val);
                            }
                            break;

                        case DELETE:
                            cache.delete(nid);
                            break;

                        default:
                            throw new RuntimeException("Cannot reach here");
                    }
                }

                cache.evict(ts);
                return tSampleReqCnt;
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
        totalReqCnt += reqCnt;
        totalSampleReqCnt += sampleReqCnt;

        // Update statistics variables
        int minute = (int) (ts / 60L / 1000L / 1000L);
        minuteList.add(minute);
        double[] mrs = new double[miniCacheCount];
        long[] bytesMiss = new long[miniCacheCount];
        long[] cacheSizes = new long[miniCacheCount];
        for (int i = 0; i < miniCacheCount; i++) {
            long ttl = (i == 0) ? 1L : (i == 1) ? 6L : 12L * (i - 1);
            CostTTLCache cache = caches.get((int) ttl);
            Pair<Integer, Integer> tmpHitMiss = cache.getTmpHitMiss();
            int tmpHit = tmpHitMiss.getFirst(), tmpMiss = tmpHitMiss.getSecond();
            mrs[i] = tmpHit + tmpMiss != 0 ? (double) tmpMiss / (tmpHit + tmpMiss) : -1.;
            bytesMiss[i] = (long) (cache.getTmpBytesMiss() / samplingRatio);
            cacheSizes[i] = (long) (cache.getOccupiedSize() / samplingRatio);
            cache.resetTmps();
        }
        missRatiosList.add(mrs);
        bytesMissList.add(bytesMiss);
        cacheSizeList.add(cacheSizes);
        sampledRequestsList.add(sampleReqCnt);

        // Write to MRC file
        int idx = minuteList.size() - 1;
        for (int i = 0; i < miniCacheCount; i++) {
            int ttl = (i == 0) ? 1 : (i == 1) ? 6 : 12 * (i - 1);
            String line = String.format("%d,%d,%.6f,%s,%s\n", minute, ttl, missRatiosList.get(idx)[i], 
                                        bytesMissList.get(idx)[i], cacheSizeList.get(idx)[i]);
            writeToFile(line, mrcFilename, false);
        }

        // Write to ReqCount file
        String line = String.format("%d,%d,%d,%d,%d\n", minute, totalReqCnt, totalSampleReqCnt, reqCnt, sampleReqCnt);
        writeToFile(line, reqCntFilename, false);
    }

    /**
     * Compute the hash value of the given integer
     * 
     * @param n the integer to compute the hash value
     * @return the hash value of the given integer
     */
    private int hashId(int n) {
        String digested = DigestUtils.sha1Hex(String.valueOf(n)).substring(0, 15);
        return (int) (Long.parseLong(digested, 16) % HASH_SIZE);
    }

    /**
     * Write the given line to the given file
     * 
     * @param line the line to write
     * @param filename the file to write
     * @param isNewFile true if the file is newly created
     */
    private void writeToFile(String line, String filename, boolean isNewFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, !isNewFile))) {
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
