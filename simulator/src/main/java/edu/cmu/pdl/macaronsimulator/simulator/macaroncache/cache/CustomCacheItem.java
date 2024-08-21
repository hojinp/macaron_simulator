package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache;

public class CustomCacheItem {
    public int prev = -1;
    public int next = -1;
    public long val = -1L;
    public long birth = -1L;
    public int cacheIdx = -1;

    public void reset() {
        prev = -1;
        next = -1;
        val = -1L;
        birth = -1L;
        cacheIdx = -1;
    }
}
