package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;
import net.spy.memcached.DefaultHashAlgorithm;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * KetamaNodeLocator uses consistent hashing algorithm to implement NodeLocator.
 * This class is mostly copied from https://github.com/couchbase/spymemcached/
 */
public class KetamaNodeLocator implements NodeLocator {
    private TreeMap<Integer, String> ketamaNodes;
    private int maxHash = 100000000;

    private final KetamaNodeLocatorConfiguration config;

    public KetamaNodeLocator(final List<String> nodes) {
        this.config = new KetamaNodeLocatorConfiguration();
        setKetamaNodes(nodes);
    }

    /**
     * Return the correct node name corresponding to the given key value
     */
    @Override
    public String getNode(final ObjContent obj) {
        int k = obj.getObjId();
        int hash = hashId(String.valueOf(k));
        String rv = getNodeForKey(hash);
        assert rv != null : "Found no node for key " + k;
        return rv;
    }

    @Override
    public String getNode(final int objId) {
        int hash = hashId(String.valueOf(objId));
        String rv = getNodeForKey(hash);
        assert rv != null : "Found no node for key " + objId;
        return rv;
    }

    @Override
    public String getNode() {
        throw new RuntimeException("Cannot reach here");
    }

    /**
     * Helper function used in the getPrimary function
     * @param hash hash value of the key
     * @return Ketama node name corresponding to the hashed key value
     */
    String getNodeForKey(int hash) {
        final String rv;
        if (!ketamaNodes.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = getKetamaNodes().tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = getKetamaNodes().firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }
        rv = getKetamaNodes().get(hash);
        return rv;
    }

    public TreeMap<Integer, String> getKetamaNodes() {
        return ketamaNodes;
    }

    /**
     * Add the list of nodes to this consistent hashing NodeLocator.
     * 
     * @param nodes nodes that are added to the consistent hashing
     */
    protected void setKetamaNodes(final List<String> nodes) {
        TreeMap<Integer, String> newNodeMap = new TreeMap<>();
        int numReps = config.getNodeRepetitions();
        for (final String node : nodes) {
            for (int i = 0; i < numReps / 1; i++) {
                for (int position : ketamaNodePositionsAtIteration(node, i)) {
                    if (newNodeMap.containsKey(position))
                        Logger.getGlobal()
                                .info("Alert! Hashing generates the same position!!! : " + String.valueOf(position));
                    newNodeMap.put(position, node);
                }
            }
        }
        assert newNodeMap.size() == numReps * nodes.size() : "newNodeMap is not correctly created";
        ketamaNodes = newNodeMap;
    }

    private List<Integer> ketamaNodePositionsAtIteration(final String node, final int iteration) {
        List<Integer> positions = new ArrayList<Integer>();
        for (int h = 0; h < 1; h++) {
            int k = hashId(config.getKeyForNode(node, iteration));
            positions.add(k);
        }
        return positions;
    }

    private int hashId(String n) {
        String digested = DigestUtils.sha1Hex(n).substring(0, 15);
        return (int) (Long.parseLong(digested, 16) % maxHash);
    }

    public String getType() {
        return "Ketama";
    }
}
