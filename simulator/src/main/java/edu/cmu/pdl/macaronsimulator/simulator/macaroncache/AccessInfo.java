package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import edu.cmu.pdl.macaronsimulator.simulator.message.QType;

/**
 * AccessInfo class. This object is used to store the access history that is be used by Controller and OSCM server.
 */
public class AccessInfo {
    public QType qType = null; /* query type */
    public int objectId = -1; /* object id */
    public long val = -1L; /* object size */
    public long ts = -1L; /* timestamp */

    public void set(QType qType, int objectId, long val, long ts) {
        this.qType = qType;
        this.objectId = objectId;
        this.val = val;
        this.ts = ts;
    }
}
