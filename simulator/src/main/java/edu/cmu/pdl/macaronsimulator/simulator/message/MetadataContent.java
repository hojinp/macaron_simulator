package edu.cmu.pdl.macaronsimulator.simulator.message;

import java.util.Map;

public class MetadataContent implements Content {
    private final int blockId;
    private Map<Integer, Long> packingMap = null;

    public MetadataContent(final int blockId, final Map<Integer, Long> packingMap) {
        this.blockId = blockId;
        this.packingMap = packingMap;
    }

    /**
     * Get block ID of the block.
     * 
     * @return block ID
     */
    public int getBlockId() {
        return blockId;
    }

    /**
     * Get packing map of the block.
     * 
     * @return packing map
     */
    public Map<Integer, Long> getPackingMap() {
        if (packingMap == null)
            throw new RuntimeException("Packing map is not initialized");
        return packingMap;
    }

    @Override
    public ContentType getContentType() {
        return ContentType.METADATA;
    }
}
