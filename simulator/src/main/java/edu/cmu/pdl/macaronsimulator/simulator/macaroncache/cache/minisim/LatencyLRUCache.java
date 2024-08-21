package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;

public class LatencyLRUCache implements MiniSimCache {

    private static final int MAX_TIME_COUNT = (int) 1e9;
    private CustomCacheItem cacheItems[];
    private int head = -1;
    private int tail = -1;

    private long cacheSize;
    private long occupiedSize = 0L;

    public LatencyLRUCache(final long cacheSize, final int maxObjectId) {
        if (cacheSize <= 0L || maxObjectId >= MAX_TIME_COUNT)
            throw new RuntimeException("Cache size > 0 and max object ID < " + MAX_TIME_COUNT);
        this.cacheSize = cacheSize;
        int arrayLength = maxObjectId + 1;
        cacheItems = new CustomCacheItem[arrayLength];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < arrayLength; i += threadLength)
                cacheItems[i] = new CustomCacheItem();
        });
    }

    @Override
    public long get(int n, long ts) {
        if (cacheItems[n].val == -1L) {
            return 0L; // If key does not exist, return 0L
        }
        if (head == n)
            return cacheItems[n].val;
        int prev = cacheItems[n].prev, next = cacheItems[n].next;
        if (prev != -1)
            cacheItems[prev].next = next;
        if (next != -1)
            cacheItems[next].prev = prev;
        cacheItems[head].prev = n;
        cacheItems[n].prev = -1;
        cacheItems[n].next = head;
        if (tail == n)
            tail = prev;
        head = n;
        return cacheItems[n].val;
    }

    @Override
    public void putWithoutEviction(final int key, final long value, final long ts) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Integer> put(int n, long value, long ts) {
        if (value <= 0L)
            throw new RuntimeException("Object size must be greater than 0 and less than cache size");

        if (value >= cacheSize)
            return null;

        // 1. Insert the item <key, value>
        if (head == n) { // key exists & key is head
            occupiedSize = occupiedSize - cacheItems[n].val + value;
            cacheItems[n].val = value;
        } else {
            if (cacheItems[n].val != -1L) { // key exists & key is not head
                occupiedSize = occupiedSize - cacheItems[n].val + value;
                int prev = cacheItems[n].prev, next = cacheItems[n].next;
                cacheItems[prev].next = next;
                if (next != -1)
                    cacheItems[next].prev = prev;
                else
                    tail = prev;
            } else { // key does not exist
                occupiedSize += value;
                cacheItems[n].birth = ts;
            }
            cacheItems[n].val = value;
            cacheItems[n].prev = -1;
            cacheItems[n].next = head;
            if (head == -1) { // empty cache
                assert tail == -1 : "if head == 1, tail must be -1";
                head = tail = n;
            } else { // non-empty cache
                cacheItems[head].prev = n;
                head = n;
            }
        }

        // 2. If occupied size > cache size, evict items until occupied size <= cache size
        List<Integer> evicted = new ArrayList<>();
        while (occupiedSize > cacheSize) {
            evicted.add(tail);
            long itemSize = cacheItems[tail].val;
            cacheItems[tail].val = -1L;
            cacheItems[tail].birth = -1L;
            tail = cacheItems[tail].prev;
            assert tail != -1;
            cacheItems[tail].next = -1;
            occupiedSize -= itemSize;
        }
        return evicted;
    }

    @Override
    public long delete(int n) {
        if (cacheItems[n].val == -1L)
            return -1L;
        long itemSize = cacheItems[n].val;
        occupiedSize -= itemSize;
        cacheItems[n].val = -1L;
        cacheItems[n].birth = -1L;
        int prev = cacheItems[n].prev, next = cacheItems[n].next;
        if (head == n)
            head = next;
        if (tail == n)
            tail = prev;
        if (prev != -1)
            cacheItems[prev].next = next;
        if (next != -1)
            cacheItems[next].prev = prev;
        return itemSize;
    }

    @Override
    public List<Integer> evict(long ts) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Get the birth time of the object with ID n
     * 
     * @param n object ID
     * @return birth time of the object with ID n
     */
    public long getBirth(int n) {
        return cacheItems[n].birth;
    }

    /**
     * Update the cache size and evict items if necessary
     * 
     * @param newCacheSize new cache size
     */
    public void updateCacheSize(long newCacheSize) {
        if (cacheSize == newCacheSize)
            return;

        if (cacheSize < newCacheSize) {
            cacheSize = newCacheSize;
            return;
        }

        cacheSize = newCacheSize;
        while (occupiedSize > cacheSize) {
            long itemSize = cacheItems[tail].val;
            cacheItems[tail].val = -1L;
            cacheItems[tail].birth = -1L;
            tail = cacheItems[tail].prev;
            assert tail != -1;
            cacheItems[tail].next = -1;
            occupiedSize -= itemSize;
        }
    }

    @Override
    public long getOccupiedSize() {
        throw new UnsupportedOperationException("Unimplemented method 'getOccupiedSize'");
    }

    @Override
    public void addBytesMiss(long size) {
        throw new UnsupportedOperationException("Unimplemented method 'addBytesMiss'");
    }

    @Override
    public long getTmpBytesMiss() {
        throw new UnsupportedOperationException("Unimplemented method 'getTmpBytesMiss'");
    }

    @Override
    public Pair<Integer, Integer> getTmpHitMiss() {
        throw new UnsupportedOperationException("Unimplemented method 'getTmpHitMiss'");
    }

    @Override
    public Pair<Integer, Integer> getHitMiss() {
        throw new UnsupportedOperationException("Unimplemented method 'getHitMiss'");
    }
}
