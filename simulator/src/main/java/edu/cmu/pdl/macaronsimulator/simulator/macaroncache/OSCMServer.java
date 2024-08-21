package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import java.util.logging.Logger;
import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc.OSCCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc.OSCLRUCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.osc.OSCTTLCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.KetamaNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MetadataContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class OSCMServer implements Component {

    private final static String NAME = "OSCM_SERVER";
    private static int globalBlockId = 0;

    /* Packing is enabled or not */
    private final boolean packing;

    /* BlockId: index of the BlockIdToObjects list, Block information: List.of(Triple<ObjectId, ObjectSize, Valid>) */
    private int objIdToBlockId[];
    private OSCMItem blockIdToItems[][];
    private long blockIdToOccupied[];
    private long blockIdToSize[];

    /* If packing is disabled */
    private Long dataStorage[];

    private final LatencyGenerator lg;
    private final ProfileCounter profileCounter;

    private final int newEventMaxCnt = 100000000;
    private int newEventIdx = 0;
    private final Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    /* Cache related variables */
    private final OSCCache cache;

    /* Garbage collection related variables */
    private long nextGCTime = 0L;
    private Set<Integer> gcSet = new HashSet<>();
    private Map<Integer, Integer> oldToNewBlockId = new HashMap<>();
    private Map<Integer, Integer> newToOldBlockId = new HashMap<>();
    public static double GC_THRESHOLD = 0.5;

    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public OSCMServer(int maxObjectId) {
        MacConf.OSCM_NAME = NAME;
        packing = MacConf.PACKING;
        lg = new LatencyGenerator();
        profileCounter = new ProfileCounter();
        cache = getCache(maxObjectId);
        this.nextGCTime = ReconfigEvent.oscReconfigStTime;

        if (packing) {
            int length1 = maxObjectId + 1, length2 = CacheEngine.MAX_BLOCK_ID + 1;
            objIdToBlockId = new int[length1];
            blockIdToItems = new OSCMItem[length2][];
            blockIdToOccupied = new long[length2];
            blockIdToSize = new long[length2];

            TaskParallelRunner.run((threadId, threadLength) -> {
                for (int j = threadId; j < length1; j += threadLength)
                    objIdToBlockId[j] = -1;
                for (int j = threadId; j < length2; j += threadLength) {
                    blockIdToItems[j] = null;
                    blockIdToOccupied[j] = -1;
                    blockIdToSize[j] = -1;
                }
            });
        } else {
            int length = maxObjectId + 1;
            dataStorage = new Long[length];
            TaskParallelRunner.run((threadId, threadLength) -> {
                for (int j = threadId; j < length; j += threadLength)
                    dataStorage[j] = -1L;
            });
        }

        // Register message handlers
        msgHandlers.put(CompType.ENGINE, this::cacheEngineMsgHandler);
        msgHandlers.put(CompType.CONTROLLER, this::controllerMsgHandler);
        msgHandlers.put(CompType.OSC, this::oscMsgHandler);
    }

    /**
     * Create a cache based on the cache type.
     * 
     * @param maxObjectId maximum object id that can be stored in the cache
     * @return cache instance
     */
    private static OSCCache getCache(int maxObjectId) {
        switch (MacConf.OSC_CACHE_TYPE) {
            case LRU:
                Logger.getGlobal().info("LRU OSC cache is created.");
                return new OSCLRUCache(MacConf.DEFAULT_OSC_CAPACITY, maxObjectId);
            case TTL:
                Logger.getGlobal().info("TTL OSC cache is created.");
                return new OSCTTLCache(MacConf.TTL, maxObjectId);
            default:
                throw new IllegalArgumentException("CacheType=" + MacConf.OSC_CACHE_TYPE + " is not supported yet!");
        }
    }

    @Override
    public CompType getComponentType() {
        return CompType.OSCM;
    }

    /**
     * Generate a new block id.
     * @return new block id
     */
    public static int genBlockId() {
        assert globalBlockId < Integer.MAX_VALUE - 1;
        return globalBlockId++;
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(long ts, Message msg) {
        newEventIdx = 0;
        msgHandlers.get(msg.getFlow() == MsgFlow.RECV ? msg.getSrcType() : msg.getDstType()).accept(ts, msg);
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * Handles messages coming from/going to the CacheEngines.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the ProxyEngine
     */
    private void cacheEngineMsgHandler(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Message type must be REQ");
                int blockId, objectId;
                CompType srcType = msg.getSrcType();
                long objSize = -1L;
                switch (msg.getQType()) {
                    case GET:
                    case PREFETCH_GET:
                        if (msg.getQType() == QType.GET)
                            profileCounter.queryCounter(QType.GET);

                        objectId = msg.getObjId();
                        if (packing) {
                            blockId = objIdToBlockId[objectId];
                            if (msg.getQType() == QType.GET)
                                profileCounter.cacheCounter(blockId == -1 ? false : true);

                            if (blockId == -1) { // the object is not in the OSC
                                addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                        msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                                break;
                            }

                            OSCMItem[] items = blockIdToItems[blockId];
                            if (items == null)
                                throw new RuntimeException("Block must exist in the block list");
                            for (int i = 0; i < items.length; i++) {
                                if (items[i].objectId == objectId) {
                                    objSize = items[i].size;
                                    restoreEvictedItemIfNeeded(blockId, items[i]);
                                    break;
                                }
                            }
                            if (objSize == -1L)
                                throw new RuntimeException("Item must exist in the item list");
                            addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                    msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst().setDataExists()
                                            .setBlockId(blockId).setObjSize(objSize)));
                        } else {
                            objSize = dataStorage[objectId];
                            if (msg.getQType() == QType.GET)
                                profileCounter.cacheCounter(objSize == -1L ? false : true);

                            if (objSize == -1L) { // the object is not in the OSC
                                addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                        msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                                break;
                            }

                            addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                    msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst().setDataExists()
                                            .setObjSize(objSize)));
                        }
                        break;

                    case PUT:
                        profileCounter.queryCounter(QType.PUT);
                        if (packing) {
                            MetadataContent metadata = msg.getMdata();
                            blockId = metadata.getBlockId();
                            Map<Integer, Long> block = metadata.getPackingMap();
                            for (int oid : block.keySet())
                                deprecateObject(oid, OSCMItemState.DELETED);

                            blockIdToItems[blockId] = block.entrySet().stream()
                                    .map(entry -> {
                                        objIdToBlockId[entry.getKey()] = blockId;
                                        return new OSCMItem(entry.getKey(), entry.getValue(), OSCMItemState.VALID);
                                    })
                                    .toArray(OSCMItem[]::new);
                            long blockSize = block.values().stream().mapToLong(Long::longValue).sum();
                            blockIdToOccupied[blockId] = blockSize;
                            blockIdToSize[blockId] = blockSize;
                        } else {
                            dataStorage[msg.getObjId()] = msg.getObjSize();
                        }
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case DELETE:
                        profileCounter.queryCounter(QType.DELETE);
                        if (packing) {
                            long size = deprecateObject(msg.getObjId(), OSCMItemState.DELETED);
                            if (size != -1L)
                                profileCounter.query(QType.DELETE, size);
                        } else {
                            dataStorage[msg.getObjId()] = -1L;
                        }
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.OSCM, srcType, 0L),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Message type must be RESP");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Deprecate the object from the OSCMServer (only used for packing==True)
     * 
     * @param objectId the object that should be deprecated
     * @param state the state that the object should be deprecated to
     * @return -1 if block doesn't exist, 0 if the item is already invalid, size if the item is valid to invalid
     */
    private long deprecateObject(final int objectId, final OSCMItemState state) {
        if (!packing || (state != OSCMItemState.EVICTED && state != OSCMItemState.DELETED))
            throw new RuntimeException("Packing must be enabled and the state must be EVICTED or DELETED");

        int blockId = objIdToBlockId[objectId];

        if (blockId == -1)
            return -1L;

        final OSCMItem items[] = blockIdToItems[blockId];
        long objSize = -1L;
        for (int i = 0; i < items.length; i++) {
            if (items[i].objectId == objectId) {
                objSize = items[i].state == OSCMItemState.VALID ? items[i].size : 0L;
                items[i].state = state;
                break;
            }
        }

        if (state == OSCMItemState.DELETED)
            objIdToBlockId[objectId] = -1;
        blockIdToOccupied[blockId] -= objSize;

        if (objSize == -1L || blockIdToOccupied[blockId] < 0L)
            throw new RuntimeException("Block must exist and the occupied size must be greater than 0");

        checkAndAddToGCSet(blockId, blockIdToOccupied[blockId], blockIdToSize[blockId]);

        return objSize;
    }

    /**
     * Restore the item from EVICTED state to VALID state.
     * 
     * @param blockId ID of the block that has the item
     * @param item the item that should be restored from the evicted state.
     */
    private void restoreEvictedItemIfNeeded(int blockId, OSCMItem item) {
        if (item.state == OSCMItemState.DELETED)
            throw new RuntimeException("Cannot restore the item that is already deleted");

        if (item.state == OSCMItemState.EVICTED) {
            item.state = OSCMItemState.VALID;
            blockIdToOccupied[blockId] += item.size;
        }
    }

    /**
     * Handles messages coming from/going to the Controller.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the MacaronMaster
     */
    private void controllerMsgHandler(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.REQ || msg.getQType() != QType.RECONFIG)
                    throw new RuntimeException("Message type must be REQ and QType must be RECONFIG");

                // Run logical cache that manages the cache items stored in the object store.
                for (int idx = 0; idx < TraceDataDB.totalAccessCount; idx++) {
                    AccessInfo access = TraceDataDB.accessHistory[idx];
                    if (access.qType == QType.GET) {
                        long objSize = cache.get(access.ts, access.objectId);
                        if (objSize == -1L) /* if item does not exist in the cache */
                            cache.putWithoutEviction(access.ts, access.objectId, access.val);
                    } else if (access.qType == QType.PUT) {
                        cache.putWithoutEviction(access.ts, access.objectId, access.val);
                    } else if (access.qType == QType.DELETE) {
                        cache.delete(access.objectId);
                    } else {
                        throw new RuntimeException("Unsupported query type: " + access.qType);
                    }
                }

                // If the new OSC size is set, update the cache size.
                if (MacConf.OSC_CACHE_TYPE == CacheType.LRU) {
                    if (msg.getOSCSizeMB() != -1 && ReconfigEvent.UPDATE_OSC_FIRST_EVICT_LATER) {
                        cache.setNewCacheSize(msg.getOSCSizeMB() * 1024L * 1024L);
                    }
                } else if (MacConf.OSC_CACHE_TYPE == CacheType.TTL) {
                    if (msg.getOSCTTLMin() != -1 && ReconfigEvent.UPDATE_OSC_FIRST_EVICT_LATER) {
                        cache.setNewTTL((long) msg.getOSCTTLMin() * 60L * 1000L * 1000L);
                    }
                } else {
                    throw new RuntimeException("Unsupported cache type: " + MacConf.OSC_CACHE_TYPE);
                }

                // Run lazy eviction of the cache items stored in the object store.
                long elapsed = 0L;
                if (ts >= nextGCTime) {
                    elapsed += 1L * 1000L * 1000L; /* XXX: empirical value, 1 sec for GC */

                    List<Integer> evictions = cache.evict(ts);
                    if (packing) {
                        for (int eviction : evictions)
                            deprecateObject(eviction, OSCMItemState.EVICTED);
                        run_gc(ts + elapsed);
                    } else {
                        for (int eviction : evictions) {
                            dataStorage[eviction] = -1L;
                            Message newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.DELETE, MsgType.REQ,
                                    MacConf.OSCM_NAME, CompType.OSCM, MacConf.OSC_NAME, CompType.OSC)
                                    .setObj(new ObjContent(eviction));
                            addEvent(new Pair<>(ts, newMsg));
                        }
                    }

                    if (MacConf.OSC_CACHE_TYPE == CacheType.LRU) {
                        if (msg.getOSCSizeMB() != -1 && !ReconfigEvent.UPDATE_OSC_FIRST_EVICT_LATER) {
                            cache.setNewCacheSize(msg.getOSCSizeMB() * 1024L * 1024L);
                        }
                    } else if (MacConf.OSC_CACHE_TYPE == CacheType.TTL) {
                        if (msg.getOSCTTLMin() != -1 && !ReconfigEvent.UPDATE_OSC_FIRST_EVICT_LATER) {
                            cache.setNewTTL((long) msg.getOSCTTLMin() * 60L * 1000L * 1000L);
                        }
                    } else {
                        throw new RuntimeException("Unsupported cache type: " + MacConf.OSC_CACHE_TYPE);
                    }

                    nextGCTime = nextGCTime + ReconfigEvent.getOSCGCInterval();
                }

                // If Pretetch is enabled, return the list of scanned items that should be prefetched.
                if (ReconfigEvent.IS_PREFETCH_ENABLED && msg.hasCdata()) {
                    elapsed += 1L * 1000L * 1000L; /* XXX: empirical value, 1 sec for SCAN */
                    NodeLocator nodeRoute = new KetamaNodeLocator(msg.getCdata().getCacheEngineNameList());
                    cache.scan(msg.getDramPrefetchSize(), nodeRoute);
                }

                addEvent(new Pair<>(ts + elapsed, ReconfigEvent.IS_PREFETCH_ENABLED && msg.hasCdata()
                        ? msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst().setScanOrder(
                                cache.getScanOrder(), cache.getScanOrderSize(), cache.getScanOrderNode(),
                                cache.getScanOrderCount())
                        : msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP || msg.getQType() != QType.RECONFIG)
                    throw new RuntimeException("Message type must be RESP and QType must be RECONFIG");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Handles messages coming from/going to the OSC.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the OSC
     */
    private void oscMsgHandler(final long ts, final Message msg) {
        Message newMsg;
        int oldBlockId, newBlockId;
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Message type must be RESP");

                switch (msg.getQType()) {
                    case GET: // GC: after reading the old block, put the new packing block to the OSC
                        if (!packing)
                            throw new RuntimeException("Packing must be enabled for GC GET");

                        oldBlockId = msg.getBlockId();
                        newBlockId = oldToNewBlockId.remove(oldBlockId);
                        newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.PUT, MsgType.REQ,
                                MacConf.OSCM_NAME, CompType.OSCM, MacConf.OSC_NAME, CompType.OSC).setBlockId(newBlockId)
                                .setBlockSize(blockIdToSize[newBlockId]);
                        addEvent(new Pair<>(ts, newMsg));
                        break;

                    case PUT: // GC: update the corresponding metadata and delete the old packing block from the OSC
                        if (!packing)
                            throw new RuntimeException("Packing must be enabled for GC PUT");

                        newBlockId = msg.getBlockId();
                        oldBlockId = newToOldBlockId.remove(newBlockId);
                        blockIdToItems[oldBlockId] = null;
                        gcSet.remove(oldBlockId); // If the old block is inserted into the gcSet again, remove it

                        // Update the metadata for the new block, and change the states of the items that are deleted
                        long validSize = 0L;
                        OSCMItem[] newBlock = blockIdToItems[newBlockId];
                        for (OSCMItem item : newBlock) {
                            objIdToBlockId[item.objectId] = objIdToBlockId[item.objectId] == -1 ? -1 : newBlockId;
                            item.state = objIdToBlockId[item.objectId] == -1 ? OSCMItemState.DELETED : item.state;
                            validSize += item.state == OSCMItemState.VALID ? item.size : 0L;
                        }
                        blockIdToOccupied[newBlockId] = validSize;

                        // Delete the old packing block from the OSC
                        newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.DELETE, MsgType.REQ,
                                MacConf.OSCM_NAME, CompType.OSCM, MacConf.OSC_NAME, CompType.OSC)
                                .setBlockId(oldBlockId);
                        addEvent(new Pair<>(ts, newMsg));
                        break;

                    case DELETE:
                        /* do nothing */
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Message type must be REQ");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Add the block id to the gcSet if the block should be gc'ed.
     * 
     * @param blockId block id that will be check if it should be gc'ed or not
     * @param occupiedSize occupied size of the block
     * @param blockSize total block size
     */
    private void checkAndAddToGCSet(int blockId, long occupiedSize, long blockSize) {
        if (checkToGC(occupiedSize, blockSize))
            gcSet.add(blockId);
    }

    /**
     * Check if the block should be gc'ed. The current implementation of decision making of whether we should gc or not 
     * is whether the block's capacity occupancy is less than X% or not.
     * 
     * @param occupiedSize occupied size of the block
     * @param blockSize total block size
     */
    private boolean checkToGC(long occupiedSize, long blockSize) {
        return (double) occupiedSize / (double) blockSize < GC_THRESHOLD;
    }

    /**
     * Garbage collection of packing blocks listed in the gcSet:
     * 
     * For each block in gcSet:
     * - If all items in the block are invalid, delete the packing block.
     * - If some items are valid:
     *   - Generate new metadata for the block in blockIdToItems (not immediately used).
     *   - Send the new packing block to the OSC.
     *   - Upon receiving a response from the OSC, OSCMServer starts using the new metadata.
     *   - After updating to the new block metadata, delete the packing block from the OSC.
     * 
     * @param ts timestamp when this message should be triggered
     */
    private void run_gc(long ts) {
        Message newMsg;
        for (int oldBlockId : gcSet) {
            if (!checkToGC(blockIdToOccupied[oldBlockId], blockIdToSize[oldBlockId]))
                continue;

            OSCMItem[] oldBlock = blockIdToItems[oldBlockId];
            final Map<Integer, Long> newBlock = new HashMap<>();
            for (OSCMItem item : oldBlock) {
                if (item.state == OSCMItemState.VALID)
                    newBlock.put(item.objectId, item.size);
                else if (item.state == OSCMItemState.EVICTED)
                    objIdToBlockId[item.objectId] = -1;
            }

            // No item is valid in this block
            if (newBlock.size() == 0) {
                blockIdToItems[oldBlockId] = null;
                newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.DELETE, MsgType.REQ, MacConf.OSCM_NAME,
                        CompType.OSCM, MacConf.OSC_NAME, CompType.OSC).setBlockId(oldBlockId);
                addEvent(new Pair<>(ts, newMsg));
                continue;
            }

            // New block is created and build the metadata of the new block
            int newBlockId = genBlockId();
            OSCMItem[] newItems = newBlock.entrySet().stream()
                    .map(entry -> new OSCMItem(entry.getKey(), entry.getValue(), OSCMItemState.VALID))
                    .toArray(OSCMItem[]::new);
            long blockSize = newBlock.values().stream().mapToLong(Long::longValue).sum();
            blockIdToItems[newBlockId] = newItems;
            blockIdToSize[newBlockId] = blockSize;

            // Save the mapping between old/new block ids and read the old packing block data from the OSC
            oldToNewBlockId.put(oldBlockId, newBlockId);
            newToOldBlockId.put(newBlockId, oldBlockId);
            newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.GET, MsgType.REQ, MacConf.OSCM_NAME,
                    CompType.OSCM, MacConf.OSC_NAME, CompType.OSC).setBlockId(oldBlockId);
            addEvent(new Pair<>(ts, newMsg));
        }
        gcSet.clear();
    }

    /**
     * Add a new event to the event list.
     * 
     * @param event new event to be added
     */
    private void addEvent(Pair<Long, Event> event) {
        newEvents[newEventIdx++] = event;
    }

    @Override
    public ProfileInfo getAndResetProfileInfo() {
        profileCounter.setOccupiedSize(cache.getOccupiedSize());
        profileCounter.setCapacity(cache.getCacheSize());
        return profileCounter.getAndResetProfileInfo();
    }

    @Override
    public void saveProfileInfo(long timestamp, CompType componentType, String name, ProfileInfo profileInfo) {
        throw new RuntimeException("OSCMServer:saveProfileInfo:This method must never be called.");
    }
}
