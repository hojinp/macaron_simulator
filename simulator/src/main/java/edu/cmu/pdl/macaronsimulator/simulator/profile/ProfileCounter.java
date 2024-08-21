package edu.cmu.pdl.macaronsimulator.simulator.profile;

import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;

public class ProfileCounter {
    private int putCnt = 0;
    private int getCnt = 0;
    private int deleteCnt = 0;

    private int gcPutCnt = 0;
    private int gcGetCnt = 0;
    private int gcDeleteCnt = 0;

    private long deleteBytes = 0L;

    private long recvBytes = 0L;
    private long sendBytes = 0L;

    private int hitCnt = 0;
    private int missCnt = 0;

    private long occupiedSize = 0L; // byte
    private long capacity = 0L; // byte

    private String machineType = null;

    public void setOccupiedSize(final long occupiedSize) {
        this.occupiedSize = occupiedSize;
    }

    public void setCapacity(final long capacity) {
        this.capacity = capacity;
    }

    public void setMachineType(final String machineType) {
        this.machineType = machineType;
    }

    public void queryCounter(QType queryType) {
        if (queryType == QType.PUT) {
            putCnt++;
        } else if (queryType == QType.GET) {
            getCnt++;
        } else if (queryType == QType.DELETE) {
            deleteCnt++;
        } else if (queryType == QType.GC_PUT) {
            gcPutCnt++;
        } else if (queryType == QType.GC_GET) {
            gcGetCnt++;
        } else if (queryType == QType.GC_DELETE) {
            gcDeleteCnt++;
        } else {
            throw new RuntimeException("Unexpected query type: " + queryType.toString());
        }
    }

    public void query(QType queryType, long bytes) {
        if (queryType == QType.DELETE) {
            deleteBytes += bytes;
        } else {
            throw new RuntimeException("Unexpected query type: " + queryType.toString());
        }
    }

    public void transfer(final MsgFlow messageDirection, final long bytes) {
        if (MsgFlow.RECV == messageDirection) {
            recvBytes += bytes;
        } else if (MsgFlow.SEND == messageDirection) {
            sendBytes += bytes;
        } else {
            throw new RuntimeException(":Unexpected message direction: " + messageDirection.toString());
        }
    }

    public void cacheCounter(boolean hit) {
        if (hit) {
            hitCnt++;
        } else {
            missCnt++;
        }
    }

    public ProfileInfo getAndResetProfileInfo() {
        final ProfileInfo profileInfo = new ProfileInfo(putCnt, getCnt, deleteCnt, gcPutCnt, gcGetCnt, gcDeleteCnt,
                deleteBytes, recvBytes, sendBytes, hitCnt, missCnt, occupiedSize, capacity, machineType);
        putCnt = getCnt = deleteCnt = gcPutCnt = gcGetCnt = gcDeleteCnt = hitCnt = missCnt = 0;
        deleteBytes = recvBytes = sendBytes = 0L;
        return profileInfo;
    }
}
