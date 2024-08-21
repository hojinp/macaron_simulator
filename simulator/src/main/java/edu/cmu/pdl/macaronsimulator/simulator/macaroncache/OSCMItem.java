package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

public class OSCMItem {
    public int objectId;
    public long size;
    public OSCMItemState state;

    public OSCMItem(int objectId, long size, OSCMItemState state) {
        this.objectId = objectId;
        this.size = size;
        this.state = state;
    }

    public void set(int objectId, long size, OSCMItemState state) {
        this.objectId = objectId;
        this.size = size;
        this.state = state;
    }
}
