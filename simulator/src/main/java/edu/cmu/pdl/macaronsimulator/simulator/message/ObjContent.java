package edu.cmu.pdl.macaronsimulator.simulator.message;

public class ObjContent implements Content {
    private long objectSize = -1L;
    private int objectId;

    public ObjContent(int objectId) {
        this.objectId = objectId;
    }

    public ObjContent(int objectId, long objectSize) {
        this.objectId = objectId;
        this.objectSize = objectSize;
    }

    /**
     * Get object ID of the object.
     * 
     * @return object ID
     */
    public int getObjId() {
        return objectId;
    }

    /**
     * Get object size of the object.
     * 
     * @return object size
     */
    public long getObjSize() {
        if (objectSize <= 0L)
            throw new RuntimeException("Object size must be greater than 0");
        return objectSize;
    }

    /**
     * Set object size of the object.
     * 
     * @param objectSize object size
     */
    public void setObjSize(long objectSize) {
        if (objectSize <= 0L)
            throw new RuntimeException("Object size must be greater than 0");
        this.objectSize = objectSize;
    }

    @Override
    public ContentType getContentType() {
        return ContentType.OBJECT;
    }
}
