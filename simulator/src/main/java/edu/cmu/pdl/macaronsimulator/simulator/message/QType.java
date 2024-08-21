package edu.cmu.pdl.macaronsimulator.simulator.message;

public enum QType {
    PUT, GET, DELETE, LOCK_ACQUIRE, RECONFIG, NEW_RECONFIG, ACK, TERMINATE, STEADY, CLEAR,
    PREFETCH, PREFETCH_DONE, PREFETCH_GET, PREFETCH_PUT, GC_GET, GC_PUT, GC_DELETE,
    NONE
}
