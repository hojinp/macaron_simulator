package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.dram;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.Controller;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.KetamaNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;

public class DRAMLRUCache implements DRAMCache {
    private static final int MAX_KEY_COUNT = (int) 1e9;

    /* Two slots per each item for reconfiguration feature. This cacheItems is shared across multiple LRUCaches. */
    private static CustomCacheItem cacheItems[][];
    private int head = -1;
    private int tail = -1;

    private final int cacheIdx;
    private long cacheSize;
    private long occupiedSize = 0L;

    private static boolean prepared = false;

    public DRAMLRUCache(final int cacheIdx, final long cacheSize) {
        if (cacheSize <= 0L || !prepared)
            throw new RuntimeException("Cache size must be greater than 0 and prepare() must be called before");
        this.cacheIdx = cacheIdx;
        this.cacheSize = cacheSize;
    }

    /**
     * Prepare the cacheItems for all LRUCache objects.
     * 
     * @param maxObjectId the number of items to be cached
     */
    public static void prepare(final int maxObjectId) {
        Logger.getGlobal().info("[LRUCache] Start preparing LRUCache cacheItems");
        if (prepared || maxObjectId >= MAX_KEY_COUNT)
            throw new RuntimeException("Can be prepared only once and keyCount must be less than " + MAX_KEY_COUNT);
        prepared = true;

        int cacheItemsLength = maxObjectId + 1;
        cacheItems = new CustomCacheItem[cacheItemsLength][2];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < cacheItemsLength; i += threadLength) {
                cacheItems[i][0] = new CustomCacheItem();
                cacheItems[i][1] = new CustomCacheItem();
            }
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

    /**
     * Return the slot index that has the item.
     * 
     * @param n check if the item for the decodedOid == n exists
     * @return 0, 1 - if item exists in slot 0 or 1 / -1 - if the item does not exist in both slots
     */
    private int itemExists(int n) {
        for (int i = 0; i < 2; i++)
            if (cacheItems[n][i].val != -1L && cacheItems[n][i].cacheIdx == cacheIdx)
                return i;
        return -1;
    }

    /**
     * Return the empty slot index for the item.
     * 
     * @param n item for the decodedOid == n
     * @return empty slot index if exists, else throw RuntimeException
     */
    private int getEmptySlot(int n) {
        for (int i = 0; i < 2; i++)
            if (cacheItems[n][i].val == -1L)
                return i;
        throw new RuntimeException("No empty slot is available for this decodedOid: " + String.valueOf(n));
    }

    @Override
    public long get(int n) {
        int slot = itemExists(n);
        if (slot == -1) /* If key does not exist, return -1L */
            return -1L;

        if (head == n) /* If the item is the head of the LRU */
            return cacheItems[n][slot].val;

        int prev = cacheItems[n][slot].prev, next = cacheItems[n][slot].next;
        if (prev != -1)
            cacheItems[prev][itemExists(prev)].next = next;
        if (next != -1)
            cacheItems[next][itemExists(next)].prev = prev;

        cacheItems[head][itemExists(head)].prev = n;
        cacheItems[n][slot].prev = -1;
        cacheItems[n][slot].next = head;

        tail = tail == n ? prev : tail;
        head = n;

        return cacheItems[n][slot].val;
    }

    @Override
    public List<Integer> put(int n, long value) {
        if (value >= cacheSize || value <= 0L)
            throw new RuntimeException("0 < Object size < CacheSize");

        /* 1. Insert the item <key, value> */
        int slot = itemExists(n);
        if (head == n) { /* key exists & key is head */
            if (slot == -1)
                throw new RuntimeException("Item for decodedOid " + String.valueOf(n) + " must exist in this case");
            occupiedSize = occupiedSize - cacheItems[n][slot].val + value;
            cacheItems[n][slot].val = value;
        } else {
            if (slot != -1) { /* key exists & key is not head */
                occupiedSize = occupiedSize - cacheItems[n][slot].val + value;
                int prev = cacheItems[n][slot].prev, next = cacheItems[n][slot].next;
                cacheItems[prev][itemExists(prev)].next = next;
                if (next != -1) /* if this item was not a tail */
                    cacheItems[next][itemExists(next)].prev = prev;
                else /* this item was a tail */
                    tail = prev;
            } else { /* key does not exist */
                occupiedSize += value;
            }
            slot = slot == -1 ? getEmptySlot(n) : slot;

            cacheItems[n][slot].val = value;
            cacheItems[n][slot].prev = -1;
            cacheItems[n][slot].next = head;
            cacheItems[n][slot].cacheIdx = cacheIdx;

            if (head == -1) { /* it was an empty cache, set head and tail of the LRU */
                if (tail != -1)
                    throw new RuntimeException("if head == -1, tail must be -1");
                head = tail = n;
            } else { /* if it was not an empty cache, update the head of the LRU */
                cacheItems[head][itemExists(head)].prev = n;
                head = n;
            }
        }

        // 2. If occupied size > cache size, evict items until occupied size <= cache size
        List<Integer> evicted = new ArrayList<>();
        while (occupiedSize > cacheSize) {
            evicted.add(tail);
            int tailSlot = itemExists(tail);
            if (cacheItems[tail][tailSlot].prev == -1)
                throw new RuntimeException("CacheIdx: " + String.valueOf(cacheIdx) + ", Item: " + String.valueOf(tail));
            long itemSize = cacheItems[tail][tailSlot].val;
            int prevTail = tail;
            tail = cacheItems[tail][tailSlot].prev;
            cacheItems[prevTail][tailSlot].reset();
            cacheItems[tail][itemExists(tail)].next = -1;
            occupiedSize -= itemSize;
        }
        return evicted;
    }

    @Override
    public long delete(int n) {
        int slot = itemExists(n);
        if (slot == -1) /* item does not exist */
            return -1L;

        long itemSize = cacheItems[n][slot].val;
        occupiedSize -= itemSize;
        int prev = cacheItems[n][slot].prev, next = cacheItems[n][slot].next;
        head = head == n ? next : head; /* if the item is the LRU head, update the head */
        tail = tail == n ? prev : tail; /* if the item is the LRU tail, update the tail */
        if (prev != -1)
            cacheItems[prev][itemExists(prev)].next = next;
        if (next != -1)
            cacheItems[next][itemExists(next)].prev = prev;
        cacheItems[n][slot].reset();
        return itemSize;
    }

    @Override
    public void clear() {
        if (head == -1) /* Cache is empty. Nothing to clear. */
            return;

        // Clear the cache
        int cur, nxt = head, slot;
        while (nxt != -1) {
            slot = itemExists(nxt);
            cur = nxt;
            nxt = cacheItems[nxt][slot].next;
            cacheItems[cur][slot].reset();
        }
        head = tail = -1;
        occupiedSize = 0L;
    }

    @Override
    public void clear(List<String> proxyEngineNames) {
        if (head == -1) /* If the cache is empty, there is nothing to clear. */
            return;

        // Clear the cache for the objects that are not belonged to this cache server for the new proxy engine names
        NodeLocator nodeRoute = new KetamaNodeLocator(proxyEngineNames);
        int cur, nxt = head, slot;
        while (nxt != -1) {
            slot = itemExists(nxt);
            cur = nxt;
            nxt = cacheItems[nxt][slot].next;
            if (Controller.getCacheEngineIdx(nodeRoute.getNode(cur)) != cacheIdx)
                this.delete(cur);
        }
    }
}
