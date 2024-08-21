package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.dram;

import java.util.List;

public interface DRAMCache {
    long getOccupiedSize();

    long getCacheSize();

    long get(final int key);

    List<Integer> put(final int key, final long value);

    long delete(final int key);

    void clear();

    void clear(List<String> proxyEngineNames);
}
