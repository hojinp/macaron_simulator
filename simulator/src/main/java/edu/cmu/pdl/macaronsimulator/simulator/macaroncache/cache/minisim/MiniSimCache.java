package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim;

import java.util.List;

import org.apache.commons.math3.util.Pair;

public interface MiniSimCache {
    long get(final int key, final long ts);

    void putWithoutEviction(final int key, final long value, final long ts);

    List<Integer> put(final int key, final long value, final long ts);

    long delete(final int key);

    List<Integer> evict(long ts);

    long getOccupiedSize();

    void addBytesMiss(long size);

    long getTmpBytesMiss();

    Pair<Integer, Integer> getTmpHitMiss();

    Pair<Integer, Integer> getHitMiss();
}
