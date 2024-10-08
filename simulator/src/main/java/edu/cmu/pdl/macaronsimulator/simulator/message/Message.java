package edu.cmu.pdl.macaronsimulator.simulator.message;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.event.EventType;

public class Message implements Event {
    public static final long RESERVED_ID = -1L;

    private long requestId; /* Every request generated by Application has a unique id. If not 
                               "generated" by an application, it uses RESERVED_REQUEST_ID */
    private MsgFlow direction; /* SEND or RECV */
    private QType queryType; /* PUT, GET, DELETE, LOCK_ACQUIRE, INFO, ACK, TERMINATE, STEADY, NONE */
    private MsgType messageType; /* REQUEST, RESPONSE */
    private String src; /* which component this message is sent from */
    private String dst; /* which component this message is sent to */
    private CompType srcType; /* src component type */
    private CompType dstType; /* dst component type */
    private boolean noResponse = false; /* No response to Application or not [used for cache promotion] */
    private boolean dataExists = false; /* Check whether the data is successfully retrieved and can be delivered */
    private int blockId = -1; /* Block ID for packing block */
    private long blockSize = -1L; /* Block size for packing block */
    private boolean isVisitedPrvNode = false; /* If this message is generated for reconfig. (i.e., accessing previous DRAM) */
    private CompType dataSrc = null; /* which source this object is retrieved from */

    ObjContent objectContent = null;
    MetadataContent metadataContent = null;
    ClusterInfoContent clusterInfoContent = null;

    /* Prefetch related variables */
    private int[] scanOrder;
    private long[] scanOrderSize;
    private int[] scanOrderNode;
    private int scanOrderCount;
    private long dramPrefetchSize = -1L; /* Data size that should be prefetched */
    private long dramOccupiedSize;
    private long dramSize;

    /* OSC size reconfiguration related variables */
    private int oscSizeMB = -1;
    private int oscTTLMin = -1;

    public Message() {
    }

    public Message(final long requestId, final MsgFlow direction, final QType queryType, final MsgType messageType,
            final String src, final CompType srcType, final String dst, final CompType dstType) {
        this.requestId = requestId;
        this.direction = direction;
        this.queryType = queryType;
        this.messageType = messageType;
        this.src = src;
        this.srcType = srcType;
        this.dst = dst;
        this.dstType = dstType;
    }

    public Message reset() {
        requestId = 0L;
        direction = null;
        queryType = null;
        messageType = null;
        src = null;
        dst = null;
        srcType = null;
        dstType = null;
        noResponse = false;
        dataExists = false;
        blockId = -1;
        blockSize = -1L;
        isVisitedPrvNode = false;
        objectContent = null;
        metadataContent = null;
        clusterInfoContent = null;
        scanOrder = null;
        scanOrderSize = null;
        scanOrderNode = null;
        scanOrderCount = 0;
        dramPrefetchSize = -1L;
        dramOccupiedSize = 0L;
        dramSize = 0L;
        dataSrc = null;
        return this;
    }

    public Message set(final long requestId, final MsgFlow direction, final QType queryType, final MsgType messageType,
            final String src, final CompType srcType, final String dst, final CompType dstType) {
        this.requestId = requestId;
        this.direction = direction;
        this.queryType = queryType;
        this.messageType = messageType;
        this.src = src;
        this.srcType = srcType;
        this.dst = dst;
        this.dstType = dstType;
        return this;
    }

    @Override
    public EventType getEventType() {
        return EventType.MESSAGE;
    }

    public long getReqId() {
        return requestId;
    }

    public MsgFlow getFlow() {
        return direction;
    }

    public Message setFlow(final MsgFlow messageDirection) {
        this.direction = messageDirection;
        return this;
    }

    public QType getQType() {
        return queryType;
    }

    public Message setQType(final QType queryType) {
        this.queryType = queryType;
        return this;
    }

    public MsgType getMsgType() {
        return messageType;
    }

    public Message setMsgType(final MsgType messageType) {
        this.messageType = messageType;
        return this;
    }

    public String getSrc() {
        return src;
    }

    public CompType getSrcType() {
        return srcType;
    }

    public Message setSrc(final String src, final CompType srcType) {
        this.src = src;
        this.srcType = srcType;
        return this;
    }

    public String getDst() {
        return dst;
    }

    public CompType getDstType() {
        return dstType;
    }

    public Message setDst(final String dst, final CompType dstType) {
        this.dst = dst;
        this.dstType = dstType;
        return this;
    }

    public boolean getNoResp() {
        return noResponse;
    }

    public Message setNoResp() {
        this.noResponse = true;
        return this;
    }

    public boolean getDataExists() {
        return dataExists;
    }

    public Message setDataExists() {
        this.dataExists = true;
        return this;
    }

    public Message setDataNoExists() {
        this.dataExists = false;
        return this;
    }

    public int getBlockId() {
        assert blockId != -1;
        return blockId;
    }

    public Message setBlockId(int blockId) {
        this.blockId = blockId;
        return this;
    }

    public Message resetBlockId() {
        this.blockId = -1;
        return this;
    }

    public long getBlockSize() {
        assert blockSize != -1L;
        return blockSize;
    }

    public Message setBlockSize(long blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public boolean visitedPrvNode() {
        return isVisitedPrvNode;
    }

    public Message setVisitedPrvNode(boolean visited) {
        this.isVisitedPrvNode = visited;
        return this;
    }

    public Message swapSrcDst() {
        String tmpString = src;
        CompType tmpType = srcType;
        src = dst;
        dst = tmpString;
        srcType = dstType;
        dstType = tmpType;
        return this;
    }

    public ObjContent getObj() {
        assert objectContent != null;
        return objectContent;
    }

    public Message setObj(final ObjContent content) {
        this.objectContent = content;
        return this;
    }

    public int getObjId() {
        assert objectContent != null;
        return objectContent.getObjId();
    }

    public long getObjSize() {
        assert objectContent != null;
        return objectContent.getObjSize();
    }

    public Message setObjSize(long objectSize) {
        assert objectContent != null;
        objectContent.setObjSize(objectSize);
        return this;
    }

    public MetadataContent getMdata() {
        assert metadataContent != null;
        return metadataContent;
    }

    public Message setMdata(final MetadataContent content) {
        this.metadataContent = content;
        return this;
    }

    public boolean hasCdata() {
        return clusterInfoContent != null;
    }

    public ClusterInfoContent getCdata() {
        assert clusterInfoContent != null;
        return clusterInfoContent;
    }

    public Message setCdata(final ClusterInfoContent content) {
        this.clusterInfoContent = content;
        return this;
    }

    public long getDramPrefetchSize() {
        assert dramPrefetchSize > 0L;
        return dramPrefetchSize;
    }

    public Message setDRAMPrefetchSize(long dramPrefetchSize) {
        this.dramPrefetchSize = dramPrefetchSize;
        return this;
    }

    public int[] getScanOrder() {
        return scanOrder;
    }

    public long[] getScanOrderSize() {
        return scanOrderSize;
    }

    public int[] getScanOrderNode() {
        return scanOrderNode;
    }

    public int getScanOrderCount() {
        return scanOrderCount;
    }

    public Message setScanOrder(int[] scanOrder, long[] scanOrderSize, int[] scanOrderNode, int scanOrderCount) {
        this.scanOrder = scanOrder;
        this.scanOrderSize = scanOrderSize;
        this.scanOrderNode = scanOrderNode;
        this.scanOrderCount = scanOrderCount;
        return this;
    }

    public Pair<Long, Long> getDRAMOccupancy() {
        return new Pair<>(dramOccupiedSize, dramSize);
    }

    public Message setDRAMOccupancy(long imcOccupiedSize, long imcSize) {
        this.dramOccupiedSize = imcOccupiedSize;
        this.dramSize = imcSize;
        return this;
    }

    public int getOSCSizeMB() {
        return oscSizeMB;
    }

    public int getOSCTTLMin() {
        return oscTTLMin;
    }

    public Message setOSCSizeMB(int oscSizeMB) {
        this.oscSizeMB = oscSizeMB;
        return this;
    }

    public Message setOSCTTLMin(int oscTTLMin) {
        this.oscTTLMin = oscTTLMin;
        return this;
    }

    public CompType getDataSrc() {
        return dataSrc;
    }

    public Message setDataSrc(CompType dataSrc) {
        this.dataSrc = dataSrc;
        return this;
    }
}
