package edu.cmu.pdl.macaronsimulator.simulator.profile;

public class ProfileInfo {
    private final int putCnt;
    private final int getCnt;
    private final int deleteCnt;

    private final int gcPutCnt;
    private final int gcGetCnt;
    private final int gcDeleteCnt;

    private final long recvBytes;
    private final long sendBytes;

    private final long deleteBytes;

    private final int hitCnt;
    private final int missCnt;

    private final long occupiedSize; // byte
    private final long capacity; // byte

    private final String machineType;

    public ProfileInfo(int putCnt, int getCnt, int deleteCnt, int gcPutCnt, int gcGetCnt, int gcDeleteCnt,
            long deleteBytes, long recvBytes, long sendBytes, int hitCnt, int missCnt, long occupiedSize, long capacity,
            String machineType) {
        this.putCnt = putCnt;
        this.getCnt = getCnt;
        this.deleteCnt = deleteCnt;

        this.gcPutCnt = gcPutCnt;
        this.gcGetCnt = gcGetCnt;
        this.gcDeleteCnt = gcDeleteCnt;

        this.deleteBytes = deleteBytes;

        this.recvBytes = recvBytes;
        this.sendBytes = sendBytes;

        this.hitCnt = hitCnt;
        this.missCnt = missCnt;

        this.occupiedSize = occupiedSize;
        this.capacity = capacity;

        this.machineType = machineType;
    }

    public static String getHeaderStr() {
        return "Time(min),MachineType,Put,Get,Delete,GCPut,GCGet,GCDelete,DeleteBytes,RecvBytes,SendBytes,Hit,Miss,OccupiedSize,Capacity\n";
    }

    public String toString(int hr) {
        return String.format("%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", hr, machineType, putCnt, getCnt,
                deleteCnt, gcPutCnt, gcGetCnt, gcDeleteCnt, deleteBytes, recvBytes, sendBytes, hitCnt, missCnt,
                occupiedSize, capacity);
    }

    public int getPutCnt() {
        return putCnt;
    }

    public int getGetCnt() {
        return getCnt;
    }

    public int getGCPutCnt() {
        return gcPutCnt;
    }

    public int getGCGetCnt() {
        return gcGetCnt;
    }

    public long getRecvBytes() {
        return recvBytes;
    }

    public long getSendBytes() {
        return sendBytes;
    }

    public long getOccupiedSize() {
        return occupiedSize; // To be changed into integral
    }

    public long getCapacity() {
        return capacity;
    }

    public String getMachineType() {
        return machineType;
    }
}
