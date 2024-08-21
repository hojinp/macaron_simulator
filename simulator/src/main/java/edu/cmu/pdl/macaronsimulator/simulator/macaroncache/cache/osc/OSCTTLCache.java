package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc;

import java.util.ArrayList;
import java.util.List;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.Controller;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OSCTTLCache implements OSCCache {

    private static final int MAX_KEY_COUNT = (int) 1e9;
    private CustomCacheItem cacheItems[];

    private long ttl;
    private long occupiedSize = 0L;

    public OSCTTLCache(final long ttl, final int maxObjectId) {
        if (maxObjectId >= MAX_KEY_COUNT || ttl < 0L)
            throw new IllegalArgumentException("keyCount < " + MAX_KEY_COUNT + " or ttl (" + ttl + ") < 0");
        this.ttl = ttl;
        int arrayLength = maxObjectId + 1;
        this.cacheItems = new CustomCacheItem[arrayLength];

        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < arrayLength; i += threadLength)
                cacheItems[i] = new CustomCacheItem();
        });
    }

    @Override
    public long getOccupiedSize() {
        return occupiedSize;
    }

    @Override
    public long getCacheSize() {
        return occupiedSize;
    }

    @Override
    public long get(long ts, int n) {
        // If key does not exist, return -1L
        if (cacheItems[n].val == -1L)
            return -1L;
        cacheItems[n].birth = ts;
        return cacheItems[n].val;
    }

    @Override
    public void putWithoutEviction(long ts, int n, long value) {
        if (value <= 0L)
            throw new IllegalArgumentException("Object size must be greater than 0 and less than cache size");

        if (cacheItems[n].val != -1L)
            occupiedSize -= cacheItems[n].val;
        cacheItems[n].birth = ts;
        cacheItems[n].val = value;
        occupiedSize += value;
    }

    @Override
    public List<Integer> put(int n, long value) {
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
    public void setNewCacheSize(long newCacheSize) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setNewTTL(long newTTL) {
        ttl = newTTL;
    }

    @Override
    public List<Integer> evict(long ts) {
        List<Integer> evicted = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final int threadId = i;
            executor.execute(() -> {
                long totalEvictedSize = 0L;
                List<Integer> evictedLocal = new ArrayList<>();
                for (int j = threadId; j < cacheItems.length; j += Runtime.getRuntime().availableProcessors()) {
                    if (cacheItems[j].val == -1L)
                        continue;
                    if (ts - cacheItems[j].birth > ttl) {
                        evictedLocal.add(j);
                        totalEvictedSize += cacheItems[j].val;
                        cacheItems[j].val = -1L;
                        cacheItems[j].birth = -1L;
                    }
                }
                synchronized (evicted) {
                    evicted.addAll(evictedLocal);
                    occupiedSize -= totalEvictedSize;
                }
            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }

        return evicted;
    }

    @Override
    public boolean exists(int n) {
        return cacheItems[n].val != -1L;
    }

    @Override
    public void scan(long dataSize, NodeLocator nodeRoute) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int[] getScanOrder() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long[] getScanOrderSize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int[] getScanOrderNode() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getScanOrderCount() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
