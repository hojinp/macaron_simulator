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
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim.CostLRUCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class CostMiniatureSimulation {

    private static int HASH_SIZE = (int) Math.pow(10, 8);
    private static long MB_TO_BYTE = 1024L * 1024L;

    private double samplingRatio;
    private int sampledHashSpace;
    private int miniCacheCount = 0;
    private int cacheSizeUnitMB = 0;

    private Map<Integer, CostLRUCache> caches = null;
    private int[] oidToNid;

    private List<Integer> minuteList = new ArrayList<>();
    private List<double[]> missRatiosList = new ArrayList<>();
    private List<long[]> bytesMissList = new ArrayList<>();
    private List<Integer> sampledRequestsList = new ArrayList<>();
    private int totalReqCnt = 0, totalSampleReqCnt = 0;

    // Only used when RocksDB is enabled
    private RocksDB db = null;
    public static String dbBaseDirPath = null;

    // Log file names
    private String mrcFilename = null;
    private String reqCntFilename = null;
    private String expectedMRCFilename = null;
    private String expectedBMCFilename = null;

    public CostMiniatureSimulation(String miniSimDirname, double samplingRatio, int miniCacheCount, int cacheSizeUnitMB,
            CacheType cacheType, int maxObjectId, boolean isRocksDB) {
        this.samplingRatio = samplingRatio;
        this.miniCacheCount = miniCacheCount;
        this.sampledHashSpace = (int) (HASH_SIZE * samplingRatio);
        this.cacheSizeUnitMB = cacheSizeUnitMB;
        if (cacheType != CacheType.LRU)
            throw new RuntimeException("Unexpected cache type: " + cacheType);

        int arrayLength = maxObjectId + 1;
        this.oidToNid = new int[arrayLength];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < arrayLength; i += threadLength) {
                oidToNid[i] = i;
            }
        });

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
        try {
            if (isRocksDB && dbBaseDirPath == null)
                throw new RuntimeException("dbBaseDirPath is null");
            this.db = isRocksDB ? RocksDB.open(dbBaseDirPath) : null;
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        for (int i = 0; i < miniCacheCount; i++) {
            int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
            long sampledCacheSizeByte = (long) (((long) cacheSizeMB * MB_TO_BYTE) * samplingRatio);
            caches.put(cacheSizeMB, new CostLRUCache(String.valueOf(cacheSizeMB), sampledCacheSizeByte, newKeyMaxId,
                    isRocksDB ? db : null));
        }
        Logger.getGlobal().info("Finished initializing miniature caches");

        // Logging related variables
        if (!Files.isDirectory(Paths.get(miniSimDirname)))
            throw new RuntimeException("Directory " + miniSimDirname + " does not exist");
        this.mrcFilename = Paths.get(miniSimDirname, "mrc.csv").toString();
        this.reqCntFilename = Paths.get(miniSimDirname, "req-count.csv").toString();
        writeToFile("Time(min),CacheSize(MB),MissRatio,BytesMiss\n", mrcFilename, true);
        writeToFile("Time(min),TotalReqCnt,TotalSampleReqCnt,LocalReqCnt,LocalSampleReqCnt\n", reqCntFilename, true);
    }

    public CostMiniatureSimulation(String miniSimDirname) {
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
                int cacheSizeMB = Integer.parseInt(splits[1].strip());
                if (cacheSizeUnitMB == 0 || cacheSizeUnitMB > cacheSizeMB)
                    cacheSizeUnitMB = cacheSizeMB;
                if (cacheSizeMB / cacheSizeUnitMB > miniCacheCount)
                    miniCacheCount = cacheSizeMB / cacheSizeUnitMB;
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }

        // Read MRC file and load the pre-computed MRC results
        try (BufferedReader reader = new BufferedReader(new FileReader(mrcFilename))) {
            double[] missRatios = null;
            long[] bytesMiss = null;
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
                    }
                    curMin = minute;
                    missRatios = new double[miniCacheCount];
                    bytesMiss = new long[miniCacheCount];
                }

                int cacheSizeMB = Integer.parseInt(splits[1].strip());
                int idx = cacheSizeMB / cacheSizeUnitMB - 1;
                if (idx < 0 || idx >= miniCacheCount)
                    throw new RuntimeException("Unexpected cache size: " + cacheSizeMB);

                missRatios[idx] = Double.parseDouble(splits[2].strip());
                bytesMiss[idx] = Long.parseLong(splits[3].strip());
            }

            if (missRatios != null) {
                minuteList.add(curMin);
                missRatiosList.add(missRatios);
                bytesMissList.add(bytesMiss);
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
        writeToFile("Time(min),CacheSize(MB),ExpectedMissRatio\n", expectedMRCFilename, true);
        writeToFile("Time(min),CacheSize(MB),ExpectedBytesMiss\n", expectedBMCFilename, true);
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
            writeToFile(String.format("%d,%d,%.4f\n", curMin, (i + 1) * cacheSizeUnitMB, mrc[i]), expectedMRCFilename,
                    false);
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
            writeToFile(String.format("%d,%d,%s\n", curMin, (i + 1) * cacheSizeUnitMB, sumBytesMiss[i]),
                    expectedBMCFilename,
                    false);
        }

        return sumBytesMiss;
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
     * Return the cache sizes (MB) used in the simulation
     * 
     * @return cache sizes (MB)
     */
    public int[] getCacheSizeMBs() {
        int[] cacheSizes = new int[miniCacheCount];
        for (int i = 0; i < miniCacheCount; i++) {
            cacheSizes[i] = cacheSizeUnitMB * (i + 1);
        }
        return cacheSizes;
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
        for (final int cacheSizeMB : caches.keySet()) {
            Future<Integer> future = executorService.submit(() -> {
                final CostLRUCache cache = caches.get(cacheSizeMB);
                int tSampleReqCnt = 0;

                for (int idx = 0; idx < reqCnt; idx++) {
                    AccessInfo access = TraceDataDB.accessHistory[idx];
                    if (hashId(access.objectId) > sampledHashSpace) // spatial sampling
                        continue;

                    int nid = oidToNid[access.objectId];
                    switch (access.qType) {
                        case PUT:
                            cache.put(nid, access.val, access.ts);
                            break;

                        case GET:
                            tSampleReqCnt++;
                            long size = cache.get(nid, access.ts);
                            if (size == 0L) {
                                cache.put(nid, access.val, access.ts);
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
        for (int i = 0; i < miniCacheCount; i++) {
            int cacheSizeMB = (i + 1) * cacheSizeUnitMB;
            CostLRUCache cache = caches.get(cacheSizeMB);
            Pair<Integer, Integer> tmpHitMiss = cache.getTmpHitMiss();
            int tmpHit = tmpHitMiss.getFirst(), tmpMiss = tmpHitMiss.getSecond();
            mrs[i] = tmpHit + tmpMiss != 0 ? (double) tmpMiss / (tmpHit + tmpMiss) : -1.;
            bytesMiss[i] = (long) (cache.getTmpBytesMiss() / samplingRatio);
            cache.resetTmps();
        }
        missRatiosList.add(mrs);
        bytesMissList.add(bytesMiss);
        sampledRequestsList.add(sampleReqCnt);

        // Write to MRC file
        int idx = minuteList.size() - 1;
        for (int i = 0; i < miniCacheCount; i++) {
            String line = String.format("%d,%d,%.6f,%s\n", minute, (i + 1) * cacheSizeUnitMB,
                    missRatiosList.get(idx)[i], bytesMissList.get(idx)[i]);
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
