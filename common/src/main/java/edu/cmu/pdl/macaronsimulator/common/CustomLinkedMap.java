package edu.cmu.pdl.macaronsimulator.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.map.LinkedMap;

public class CustomLinkedMap {
    private static int MAX_SIZE = Integer.MAX_VALUE;

    private final List<LinkedMap<String, Long>> linkedMaps = new ArrayList<>();
    private final int mapCount;

    public CustomLinkedMap(final long keyCount) {
        mapCount = (int) (keyCount / (long) MAX_SIZE) + 1;
        for (int i = 0; i < mapCount; i++) {
            linkedMaps.add(new LinkedMap<>(MAX_SIZE, (float) 1.0));
        }
    }

    public void put(String key) {
        
    }
}
