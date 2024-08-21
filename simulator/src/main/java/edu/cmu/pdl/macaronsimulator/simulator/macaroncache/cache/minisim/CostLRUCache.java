package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.minisim;

import java.util.List;

import org.apache.commons.math3.util.Pair;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CustomCacheItem;

public class CostLRUCache implements MiniSimCache {
    private static final int MAX_TIME_COUNT = (int) 1e9;
    private CustomCacheItem cacheItems[];
    private int head = -1;
    private int tail = -1;

    private long cacheSize;
    private long occupiedSize = 0L;

    public long tmpBytesMiss = 0L;
    public int hit = 0, miss = 0, tmpHit = 0, tmpMiss = 0;

    private String name = null;
    private RocksDB db = null;
    private final String nullKey = genKey(-1);

    public CostLRUCache(String name, long cacheSize, int maxObjectId, RocksDB db) {
        if (cacheSize <= 0L || maxObjectId >= MAX_TIME_COUNT)
            throw new RuntimeException("Cache size > 0 and keyCount < MAX_TIME_COUNT");

        if (db == null) {
            this.cacheSize = cacheSize;
            int arrayLength = maxObjectId + 1;
            this.cacheItems = new CustomCacheItem[arrayLength];
            TaskParallelRunner.run((threadId, threadLength) -> {
                for (int i = threadId; i < arrayLength; i += threadLength) {
                    cacheItems[i] = new CustomCacheItem();
                }
            });
        } else {
            this.name = name;
            this.cacheSize = cacheSize;
            this.db = db;
        }
    }

    @Override
    public long get(int n, long ts) {
        if (db == null) {
            // If key does not exist, return 0L
            if (cacheItems[n].val == -1L) {
                markMiss();
                return 0L;
            }

            // If key exists, move it to the head of the list
            markHit();
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
        } else {
            try {
                byte[] nInfoBytes = db.get(intToBytes(n));

                // If key exists, move it to the head of the list
                if (nInfoBytes != null) {
                    markHit();
                    String nInfo = new String(nInfoBytes);
                    String[] nSplits = nInfo.split(",");
                    if (nSplits.length != 3)
                        throw new RuntimeException("Unexpected nSplits length: " + nSplits.length);
                    long size = Long.valueOf(nSplits[2]);
                    this.put(n, size, 0L);
                    return size;
                }

                // If key does not exist, return 0L
                markMiss();
                return 0L;
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
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
    public void putWithoutEviction(final int key, final long value, final long ts) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<Integer> put(int n, long value, long ts) {
        if (value <= 0L)
            throw new RuntimeException("Object size must be greater than 0 and less than cache size");

        if (value >= cacheSize)
            return null;

        if (db == null) {
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
                        throw new RuntimeException("If head == -1, tail must be -1");
                    head = tail = n;
                } else { // non-empty cache
                    cacheItems[head].prev = n;
                    head = n;
                }
            }

            // 2. If occupied size > cache size, evict items until occupied size <= cache size
            while (occupiedSize > cacheSize) {
                long itemSize = cacheItems[tail].val;
                cacheItems[tail].val = -1L;
                tail = cacheItems[tail].prev;
                assert tail != -1;
                cacheItems[tail].next = -1;
                occupiedSize -= itemSize;
            }
            return null;
        } else {
            try {
                String keyStr = genKey(n);
                byte[] keyBytes = keyStr.getBytes();
                String prv, nxt;

                // 1. Insert the item <key, value>
                if (head == -1) { // cache is empty
                    if (tail != -1)
                        throw new RuntimeException("If head == -1, tail must be -1");
                    head = tail = n;
                    prv = nxt = nullKey;
                } else { // cache is not empty
                    boolean exists;
                    exists = db.keyMayExist(keyBytes, null);
                    if (exists) {
                        this.delete(n);
                    }

                    if (head == -1) { // cache became empty because it was the unique item
                        if (tail != -1)
                            throw new RuntimeException("If head == -1, tail must be -1");
                        head = tail = n;
                        prv = nxt = nullKey;
                    } else { // still cache is not empty
                        byte[] hByte = intToBytes(head);
                        String hInfo = new String(db.get(hByte));
                        String[] hSplits = hInfo.split(",");
                        if (hSplits.length != 3)
                            throw new RuntimeException("Unexpected hSplits length: " + hSplits.length);
                        db.put(hByte, itemToBytes(keyStr, hSplits[1], hSplits[2])); // update head
                        prv = nullKey;
                        nxt = genKey(head);
                        head = n;
                    }
                }
                db.put(keyBytes, itemToBytes(prv, nxt, Long.toString(value)));
                occupiedSize += value;

                // 2. If occupied size > cache size, evict items until occupied size <= cache size
                while (occupiedSize > cacheSize) {
                    if (tail == -1)
                        throw new RuntimeException("If occupiedSize > cacheSize, tail must not be -1");
                    this.delete(tail);
                }
                return null;
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public long delete(int n) {
        if (db == null) {
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
        } else {
            try {
                byte[] keyBytes = intToBytes(n);
                byte[] nInfoBytes = db.get(keyBytes);
                if (nInfoBytes != null) {
                    String nInfo = new String(nInfoBytes);
                    String[] nSplits = nInfo.split(",");
                    if (nSplits.length != 3)
                        throw new RuntimeException("Unexpected nSplits length: " + nSplits.length);
                    String prv = nSplits[0], nxt = nSplits[1];
                    long size = Long.parseLong(nSplits[2]);

                    occupiedSize -= size;
                    db.delete(keyBytes);

                    if (prv.equals(nullKey) && nxt.equals(nullKey)) { // Single item in the cache
                        head = tail = -1;
                    } else if (prv.equals(nullKey)) { // head, > 2items
                        byte[] nxtKeyBytes = nxt.getBytes();
                        String nxtInfo = new String(db.get(nxtKeyBytes));
                        String[] nxtSplits = nxtInfo.split(",");
                        if (nxtSplits.length != 3)
                            throw new RuntimeException("Unexpected nxtSplits length: " + nxtSplits.length);
                        db.put(nxtKeyBytes, itemToBytes(nullKey, nxtSplits[1], nxtSplits[2]));
                        head = decodeKey(nxt);
                    } else if (nxt.equals(nullKey)) { // tail, > 2items
                        byte[] prvKeyBytes = prv.getBytes();
                        String prvInfo = new String(db.get(prvKeyBytes));
                        String[] prvSplits = prvInfo.split(",");
                        if (prvSplits.length != 3)
                            throw new RuntimeException("Unexpected prvSplits length: " + prvSplits.length);
                        db.put(prvKeyBytes, itemToBytes(prvSplits[0], nullKey, prvSplits[2]));
                        tail = decodeKey(prv);
                    } else { // not head, not tail, > 3items
                        byte[] prvKeyBytes = prv.getBytes(), nxtKeyBytes = nxt.getBytes();
                        String prvInfo = new String(db.get(prvKeyBytes)), nxtInfo = new String(db.get(nxtKeyBytes));
                        String[] prvSplits = prvInfo.split(","), nxtSplits = nxtInfo.split(",");
                        if (prvSplits.length != 3 || nxtSplits.length != 3)
                            throw new RuntimeException("Unexpected prvSplits length: " + prvSplits.length
                                    + " or nxtSplits length: " + nxtSplits.length);
                        db.put(prvKeyBytes, itemToBytes(prvSplits[0], nSplits[1], prvSplits[2]));
                        db.put(nxtKeyBytes, itemToBytes(nSplits[0], nxtSplits[1], nxtSplits[2]));
                    }
                    return size;
                }
                return -1L;
            } catch (RocksDBException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public List<Integer> evict(long ts) {
        throw new UnsupportedOperationException("Not implemented");
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
        throw new UnsupportedOperationException("Not implemented");
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

    /**
     * Used when RocksDB is enabled. Encode key to string.
     * @param n key
     * @return encoded key
     */
    private String genKey(int n) {
        return Integer.toString(n) + "*" + name;
    }

    /**
     * Used when RocksDB is enabled. Decode key from string.
     * @param str encoded key
     * @return decoded key
     */
    private int decodeKey(String str) {
        return Integer.parseInt(str.split("\\*")[0]);
    }

    /**
     * Used when RocksDB is enabled. Encode key to bytes.
     * @param n key
     * @return encoded key
     */
    private byte[] intToBytes(int n) {
        return genKey(n).getBytes();
    }

    /**
     * Used when RocksDB is enabled. Incode item to bytes.
     * @param prv previous key
     * @param nxt next key
     * @param size size of the item
     * @return encoded item
     */
    private byte[] itemToBytes(String prv, String nxt, String size) {
        String info = prv + "," + nxt + "," + size;
        return info.getBytes();
    }
}