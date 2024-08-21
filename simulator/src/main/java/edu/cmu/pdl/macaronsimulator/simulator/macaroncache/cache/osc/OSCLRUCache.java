package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc;

import java.util.ArrayList;
import java.util.List;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.Controller;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;

public class OSCLRUCache implements OSCCache {

    private static final int MAX_KEY_COUNT = (int) 1e9;
    private CustomCacheItem cacheItems[];
    private int head = -1, tail = -1;

    private long cacheSize, occupiedSize = 0L;

    /* Prefetch related variables */
    private int scanOrder[];
    private long scanOrderSize[];
    private int scanOrderNode[];
    private int scanOrderCount = 0;

    public OSCLRUCache(final long cacheSize, final int maxObjectId) {
        if (maxObjectId >= MAX_KEY_COUNT)
            throw new IllegalArgumentException("keyCount < " + MAX_KEY_COUNT);
        this.cacheSize = cacheSize;
        int arrayLength = maxObjectId + 1;
        this.cacheItems = new CustomCacheItem[arrayLength];
        this.scanOrder = new int[arrayLength];
        this.scanOrderSize = new long[arrayLength];
        this.scanOrderNode = new int[arrayLength];

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
        return cacheSize;
    }

    @Override
    public long get(long ts, int n) {
        // If key does not exist, return -1L
        if (cacheItems[n].val == -1L)
            return -1L;
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
    public void putWithoutEviction(long ts, int n, long value) {
        if (value >= cacheSize)
            return;
        
        if (value <= 0L)
            throw new IllegalArgumentException("Object size must be greater than 0 and less than cache size");

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
            }
            cacheItems[n].val = value;
            cacheItems[n].prev = -1;
            cacheItems[n].next = head;
            if (head == -1) { // empty cache
                if (tail != -1)
                    throw new RuntimeException("if head == -1, tail must be -1");
                head = tail = n;
            } else { // non-empty cache
                cacheItems[head].prev = n;
                head = n;
            }
        }
    }

    @Override
    public List<Integer> put(int n, long value) {
        throw new UnsupportedOperationException("Not implemented");
        // this.putWithoutEviction(n, value);
        // return this.evict();
    }

    @Override
    public long delete(int n) {
        if (cacheItems[n].val == -1L)
            return -1L;
        long itemSize = cacheItems[n].val;
        occupiedSize -= itemSize;
        cacheItems[n].val = -1L;
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
    public void setNewCacheSize(long newCacheSize) {
        cacheSize = newCacheSize;
    }

    @Override
    public void setNewTTL(long newTTL) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Integer> evict(long ts) {
        List<Integer> evicted = new ArrayList<>();
        while (occupiedSize > cacheSize && tail != -1) {
            evicted.add(tail);
            long itemSize = cacheItems[tail].val;
            cacheItems[tail].val = -1L;
            tail = cacheItems[tail].prev;
            if (tail == -1)  {
                head = -1;
            } else {
                cacheItems[tail].next = -1;
            }
            occupiedSize -= itemSize;
        }
        return evicted;
    }

    @Override
    public boolean exists(int n) {
        return cacheItems[n].val != -1L;
    }

    @Override
    public void scan(long dataSize, NodeLocator nodeRoute) {
        scanOrderCount = 0;
        if (head == -1 || dataSize <= 0L)
            return;

        // Scan the cache from head to tail and add items to scanOrder until the total size of items >= dataSize
        int ptr = head;
        long size = 0L;
        while (ptr != -1 && size < dataSize) {
            if (size + cacheItems[ptr].val > dataSize)
                break;
            scanOrder[scanOrderCount] = ptr;
            scanOrderSize[scanOrderCount] = cacheItems[ptr].val;
            scanOrderNode[scanOrderCount] = Controller.getCacheEngineIdx(nodeRoute.getNode(ptr));
            size += cacheItems[ptr].val;
            ptr = cacheItems[ptr].next;
            scanOrderCount++;
        }
    }

    @Override
    public int[] getScanOrder() {
        return scanOrder;
    }

    @Override
    public long[] getScanOrderSize() {
        return scanOrderSize;
    }

    @Override
    public int[] getScanOrderNode() {
        return scanOrderNode;
    }

    @Override
    public int getScanOrderCount() {
        return scanOrderCount;
    }
}
