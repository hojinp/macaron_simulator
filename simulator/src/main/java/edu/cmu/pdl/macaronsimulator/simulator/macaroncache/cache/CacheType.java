package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache;

/**
 * Only LRU is implemented so far.
 */
public enum CacheType {
    LRU(/*isDRAMSupported*/ true, /*isOSCSupported*/ true),
    TTL(/*isDRAMSupported*/ false, /*isOSCSupported*/ true);

    private final boolean isDRAMSupported;
    private final boolean isOSCSupported;

    private CacheType(boolean isDRAMSupported, boolean isOSCSupported) {
        this.isDRAMSupported = isDRAMSupported;
        this.isOSCSupported = isOSCSupported;
    }

    public boolean isOSCSupported() {
        return isOSCSupported;
    }

    public boolean isDRAMSupported() {
        return isDRAMSupported;
    }
}
