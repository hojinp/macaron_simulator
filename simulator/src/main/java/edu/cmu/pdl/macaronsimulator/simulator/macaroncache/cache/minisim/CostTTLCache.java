package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.math3.util.Pair;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;

public class CostTTLCache implements MiniSimCache {
    private static final int MAX_TIME_COUNT = (int) 1e9;
    private CustomCacheItem cacheItems[];

    private final long ttl;
    private long occupiedSize = 0L;

    public long tmpBytesMiss = 0L;
    public int hit = 0, miss = 0, tmpHit = 0, tmpMiss = 0;

    public CostTTLCache(long ttl, int maxObjectId) {
        if (ttl < 0L || maxObjectId >= MAX_TIME_COUNT)
            throw new RuntimeException("Cache size > 0 and keyCount < MAX_TIME_COUNT");

        this.ttl = ttl;
        int arrayLength = maxObjectId + 1;
        this.cacheItems = new CustomCacheItem[arrayLength];

        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < arrayLength; i += threadLength) {
                cacheItems[i] = new CustomCacheItem();
            }
        });
    }

    /**
     * Update hit count
     */
    private void markHit() {
        tmpHit++;
        hit++;
    }

    /**
     * Update miss count
     */
    private void markMiss() {
        tmpMiss++;
        miss++;
    }

    @Override
    public long get(int n, long ts) {
        // If key does not exist, return 0L
        if (cacheItems[n].val == -1L) {
            markMiss();
            return 0L;
        }

        markHit();
        cacheItems[n].birth = ts;
        return cacheItems[n].val;
    }

    @Override
    public void putWithoutEviction(final int key, final long value, final long ts) {
        if (value <= 0L)
            throw new IllegalArgumentException("Object size must be greater than 0 and less than cache size");

        if (cacheItems[key].val != -1L)
            occupiedSize -= cacheItems[key].val;
        cacheItems[key].birth = ts;
        cacheItems[key].val = value;
        occupiedSize += value;
    }

    @Override
    public List<Integer> put(int n, long value, long ts) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long delete(int n) {
        if (cacheItems[n].val == -1L)
            return -1L;
        long itemSize = cacheItems[n].val;
        occupiedSize -= itemSize;
        cacheItems[n].val = -1L;
        cacheItems[n].birth = -1L;
        return itemSize;
    }

    @Override
    public List<Integer> evict(long ts) {
        List<Integer> evicted = new ArrayList<>();
        for (int j = 0; j < cacheItems.length; j += 1) {
            if (cacheItems[j].val == -1L)
                continue;
            if (ts - cacheItems[j].birth > ttl) {
                evicted.add(j);
                occupiedSize -= cacheItems[j].val;
                cacheItems[j].val = -1L;
                cacheItems[j].birth = -1L;
            }
        }
        return evicted;
    }

    /**
     * Reset tmpHit, tmpMiss, tmpBytesMiss
     */
    public void resetTmps() {
        tmpHit = tmpMiss = 0;
        tmpBytesMiss = 0L;
    }

    @Override
    public long getOccupiedSize() {
        return occupiedSize;
    }

    @Override
    public void addBytesMiss(long size) {
        tmpBytesMiss += size;
    }

    @Override
    public long getTmpBytesMiss() {
        return tmpBytesMiss;
    }

    @Override
    public Pair<Integer, Integer> getTmpHitMiss() {
        return new Pair<>(tmpHit, tmpMiss);
    }

    @Override
    public Pair<Integer, Integer> getHitMiss() {
        return new Pair<>(hit, miss);
    }
}