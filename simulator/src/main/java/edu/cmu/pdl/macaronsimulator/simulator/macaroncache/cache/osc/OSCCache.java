package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc;

import java.util.List;

import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;

public interface OSCCache {
    long getOccupiedSize();

    long getCacheSize();

    long get(final long ts, final int key);

    void putWithoutEviction(final long ts, final int key, final long value);

    List<Integer> put(final int key, final long value);

    long delete(final int key);

    void setNewTTL(long newTTL);

    void setNewCacheSize(long newCacheSize);

    List<Integer> evict(long ts);

    boolean exists(final int key);

    void scan(long dataSize, NodeLocator nodeRoute);

    int[] getScanOrder();

    long[] getScanOrderSize();

    int[] getScanOrderNode();

    int getScanOrderCount();
}
