package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.commons.MachineInfoGetter;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CachingPolicy;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.InclusionPolicy;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.KetamaNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.SameMachineNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.CacheNodeState;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MetadataContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;

public class CacheEngine implements Component {

    public final String NAME;
    private final int engineId;

    public static boolean IS_DRAM_ENABLED = true;
    public static boolean IS_OSC_ENABLED = true;
    public static boolean IS_SUSPENDED = false;
    public static boolean IS_DRAM_ONLY = false;

    private NodeLocator nodeRoute = null;

    private static LatencyGenerator lg;

    private final ProfileCounter profileCounter;

    private int READY_MSG_SIZE = (int) 1e5;
    private int readyMsgIdx = 0;
    private Message[] readyMsgs;

    /* Check if all the responses for each put, delete, write-around request are received */
    private final Map<Long, Integer> putCounter = new HashMap<>();
    private final Map<Long, Integer> deleteCounter = new HashMap<>();

    private static int sharedVarSize = 0;
    private static boolean sharedVarReady = false;

    private static int oidToEngineIdx[][]; /* Which ProxyEngine corresponds to each object ID */

    private static int runningReqCounts[][]; /* How many requests are running for the given key */
    private static QType runningQueryTypes[][]; /* What type of request is running for the given key */

    private static int waitingQueueBegs[][]; /* beginning index of the waiting queue */
    private static int waitingQueueEnds[][]; /* ending index of the waiting queue */
    private static int waitingQueueLengths[][]; /* Number of waiting queries */
    private static Message waitingQueues[][][]; /* Queue for blocked requests */
    private static int waitingQueueMaxCnt = 30;

    public final static int MAX_BLOCK_ID = (int) 1e8; /* Max value for block ID */
    private static int lockAcquiring[]; /* How many locks left to be acquired for the given key */

    /* Object packing related variables */
    private static boolean isPackingEnabled;
    private int currPackingCnt = 0;
    private long currPackingSize = 0L;
    private int globalBlockId;
    private int maxPackingCnt = 40; /* Do packing for Up to 40 objects */
    private long MAX_PACKING_SIZE = 16L * 1024L * 1024L; /* Do packing up to 16MB block size */
    private int freeBlockIncr = 100;

    /* Buffer related class variables */
    private LinkedList<Message> lockAcquireMsgs = new LinkedList<>();
    private int lockAcquireMsgIncr = 1000;
    private LinkedList<Map<Integer, Long>> freeBlocks = new LinkedList<>(); /* Blocks that will be reused repeatedly */
    private Map<Integer, Map<Integer, Long>> creatingBlocks = new HashMap<>(); /* blocks that are in creating phase */
    private Map<Integer, Map<Integer, Long>> lockWaitBlocks = new HashMap<>(); /* blocks that are waiting for lock */
    private Map<Integer, Map<Integer, Long>> lockedBlocks = new HashMap<>(); /* blocks that are locked */

    private static long objIdToBlockInfos[][][] = null; /* number of items in the blocks, size of the object */

    /* Sanity check related class variables */
    private long sanityCheckTs = 13L * 60 * 1000000L; /* XXX: check sanity check after 13 min */
    private final long sanityCheckInterval = 60L * 60L * 1000000L; /* XXX: for every time interval, run sanity check */

    /* GET operation's data lake access optimization related class variables */
    private static ArrayList<Message>[][] objToDLGetWaitMsgLists = null;

    /* When packing is disabled */
    private Map<Integer, ObjContent> lockWaitObjects = new HashMap<>(); /* Objects that are waiting for lock */
    private Map<Integer, Integer> lockWaitCounts = new HashMap<>(); /* How many requests are waiting for the lock */
    private Map<Integer, ObjContent> lockedObjects = new HashMap<>(); /* Objects that are locked */

    private final int newEventMaxCnt = 2500000;
    private int newEventIdx = 0;
    private Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    /* MacServer state */
    private CacheNodeState state = CacheNodeState.STEADY;
    private List<String> prevCacheEngineNames = null;
    private NodeLocator prevNodeRoute = null;

    /* Prefetch related variables */
    private int[] scanOrder = null;
    private long[] scanOrderSize = null;
    private int[] scanOrderNode = null;
    private static final int PREFETCH_CNT_PER_SEC_GB = 60; /* XXX: assume it prefetches 10 items per GB * second */
    private int scanOrderCount = 0;

    /* Debugging */
    private static boolean DEBUG = true;
    private static String debugDirname = "/tmp/macaron/";
    private static String queueDebugFilename = debugDirname + "queue_debug.txt";
    private static String queueExtendDebugFilename = debugDirname + "queue_extend_debug.txt";
    private static String lockDebugFilename = debugDirname + "lock_debug.txt";
    private static String blockInfoFilename = debugDirname + "block_info.txt";

    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>() {
        {
            put(CompType.APP, CacheEngine.this::applicationMsgHandler);
            put(CompType.DRAM, CacheEngine.this::dramServerMsgHandler);
            put(CompType.OSCM, CacheEngine.this::oscmServerMsgHandler);
            put(CompType.OSC, CacheEngine.this::oscMsgHandler);
            put(CompType.CONTROLLER, CacheEngine.this::controllerMsgHandler);
            put(CompType.DATALAKE, CacheEngine.this::datalakeMsgHandler);
        }
    };

    public CacheEngine(final String name) {
        this.NAME = name;
        this.engineId = Controller.getCacheEngineIdx(name);
        MacConf.ENGINE_NAME_LIST.add(name);
        CacheEngine.isPackingEnabled = MacConf.PACKING;
        this.profileCounter = new ProfileCounter();
        this.globalBlockId = OSCMServer.genBlockId();
        this.prepareReadyMsgs();
        this.prepareFreeBlocks();
        this.addLockAcquireMsgsIfNeeded();

        if (IS_DRAM_ENABLED)
            this.profileCounter.setMachineType(MachineInfoGetter.getMachineInfo(MacConf.CACHE_NODE_MTYPE).getName());
        if (MacConf.INCLUSION_POLICY != InclusionPolicy.INCLUSIVE)
            throw new RuntimeException("Not implemented for " + MacConf.INCLUSION_POLICY.toString());
    }

    /**
     * Prepare the static variables that are shared among all the cache engines for memory efficiency.
     * 
     * @param maxObjectId maximum object ID of the trace
     */
    public static void prepare(int maxObjectId) {
        Logger.getGlobal().info("Start to prepare shared variables");
        if (sharedVarReady)
            throw new RuntimeException("Shared variables are already initialized");
        sharedVarReady = true;

        lg = new LatencyGenerator();

        sharedVarSize = maxObjectId + 1;
        oidToEngineIdx = new int[sharedVarSize][2];
        runningReqCounts = new int[sharedVarSize][2];
        runningQueryTypes = new QType[sharedVarSize][2];
        waitingQueues = new Message[sharedVarSize][2][];
        waitingQueueBegs = new int[sharedVarSize][2];
        waitingQueueEnds = new int[sharedVarSize][2];
        waitingQueueLengths = new int[sharedVarSize][2];
        objIdToBlockInfos = new long[sharedVarSize][2][2];
        objToDLGetWaitMsgLists = new ArrayList[sharedVarSize][2];
        if (isPackingEnabled)
            lockAcquiring = new int[MAX_BLOCK_ID];

        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < sharedVarSize; i += threadLength) {
                oidToEngineIdx[i][0] = oidToEngineIdx[i][1] = -1;

                runningReqCounts[i][0] = runningReqCounts[i][1] = 0;
                runningQueryTypes[i][0] = runningQueryTypes[i][1] = QType.NONE;

                waitingQueues[i][0] = new Message[waitingQueueMaxCnt];
                waitingQueues[i][1] = new Message[waitingQueueMaxCnt];
                waitingQueueBegs[i][0] = waitingQueueBegs[i][1] = 0;
                waitingQueueEnds[i][0] = waitingQueueEnds[i][1] = 0;
                waitingQueueLengths[i][0] = waitingQueueLengths[i][1] = 0;

                objIdToBlockInfos[i][0][0] = objIdToBlockInfos[i][0][1] = 0L;
                objIdToBlockInfos[i][1][0] = objIdToBlockInfos[i][1][1] = 0L;
                objToDLGetWaitMsgLists[i][0] = new ArrayList<Message>();
                objToDLGetWaitMsgLists[i][1] = new ArrayList<Message>();
            }

            if (isPackingEnabled)
                for (int i = threadId; i < MAX_BLOCK_ID; i += threadLength)
                    lockAcquiring[i] = 0;
        });

        if (DEBUG) {
            Logger.getGlobal().info("CacheEngine debugging is enabled.");
            try {
                java.nio.file.Files.createDirectories(java.nio.file.Paths.get(debugDirname));

                java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(queueDebugFilename));
                java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(queueExtendDebugFilename));
                writeLineToFile(queueDebugFilename, "ObjId,QueueType,ReqId,QueryType", true);
                writeLineToFile(queueExtendDebugFilename, "ObjId,OldLength,NewLength", true);

                if (isPackingEnabled) {
                    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(lockDebugFilename));
                    java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(blockInfoFilename));
                    writeLineToFile(lockDebugFilename, "LockAcquringBlockId,WaitingItemCnt", true);
                    writeLineToFile(blockInfoFilename, "BlockType,BlockId,ObjectId,ObjectSize", true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Return or assign the position of the given object ID corresponding to the cache engine.
     * 
     * @param objectId object ID to be assigned
     * @return position of the given object ID corresponding to the cache engine
     */
    private int getOrAssignOidToPos(int objectId) {
        int pos = oidToEngineIdx[objectId][0] == engineId ? 0 : oidToEngineIdx[objectId][1] == engineId ? 1 : -1;
        if (pos != -1)
            return pos;

        pos = oidToEngineIdx[objectId][0] == -1 ? 0 : oidToEngineIdx[objectId][1] == -1 ? 1 : -1;
        if (pos == -1)
            throw new RuntimeException("Object id cannot be assigned to this cache engine");
        oidToEngineIdx[objectId][pos] = engineId;
        return pos;
    }

    /**
     * Return the position of the given object ID corresponding to the cache engine. If the object ID is not assigned
     * to the cache engine, return -1.
     * 
     * @param decodedOid object ID to be assigned
     * @return position of the given object ID corresponding to the cache engine
     */
    private int getAssignedOidToPos(int objectId) {
        return oidToEngineIdx[objectId][0] == engineId ? 0 : oidToEngineIdx[objectId][1] == engineId ? 1 : -1;
    }

    /**
     * Clean up the local variables.
     */
    public void cleanUpSharedVars() {
        lockAcquireMsgs.clear();
        freeBlocks.clear();
        creatingBlocks.clear();
        lockWaitBlocks.clear();
        lockedBlocks.clear();
        lockWaitObjects.clear();
        lockWaitCounts.clear();
        lockedObjects.clear();
        newEvents = null;
        scanOrder = null;
        scanOrderSize = null;
        scanOrderNode = null;
    }

    /**
     * Clean up the assigned position of the objects that have IDs corrsponding to the cache engine.
     */
    private void clearProxyIdx() {
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < sharedVarSize; i += threadLength) {
                int pos = getAssignedOidToPos(i);
                if (pos == -1)
                    continue;
                oidToEngineIdx[i][pos] = -1;

                if (runningReqCounts[i][pos] > 0 || waitingQueueLengths[i][pos] > 0)
                    throw new RuntimeException("Cannot clean up the CacheEngine while running/waiting requests");
                runningQueryTypes[i][pos] = QType.NONE;
                waitingQueues[i][pos] = new Message[waitingQueueMaxCnt];
                waitingQueueBegs[i][pos] = waitingQueueEnds[i][pos] = 0;
                objIdToBlockInfos[i][pos][0] = objIdToBlockInfos[i][pos][1] = 0L;
                objToDLGetWaitMsgLists[i][pos] = new ArrayList<Message>();
            }
        });
    }

    /**
     * Clean up the assigned position of the objects that have IDs that are not corrsponding to the cache engine with
     * the new cache engine names.
     * 
     * @param engineNames list of cache engine names
     */
    private void cleanProxyIdx(List<String> engineNames) {
        NodeLocator nodeRoute = new KetamaNodeLocator(engineNames);
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < sharedVarSize; i += threadLength) {
                int pos = getAssignedOidToPos(i);
                if (pos == -1)
                    continue;
                if (Controller.getCacheEngineIdx(nodeRoute.getNode(i)) == engineId)
                    continue;
                oidToEngineIdx[i][pos] = -1;

                if (runningReqCounts[i][pos] > 0 || waitingQueueLengths[i][pos] > 0)
                    throw new RuntimeException("Cannot clean up the CacheEngine while running/waiting requests");
                runningQueryTypes[i][pos] = QType.NONE;
                waitingQueues[i][pos] = new Message[waitingQueueMaxCnt];
                waitingQueueBegs[i][pos] = waitingQueueEnds[i][pos] = 0;
                objIdToBlockInfos[i][pos][0] = objIdToBlockInfos[i][pos][1] = 0L;
                objToDLGetWaitMsgLists[i][pos] = new ArrayList<Message>();
            }
        });
    }

    /**
     * The cache engine only communicates with the local cache engine.
     */
    public void setNodeLocator() {
        nodeRoute = new SameMachineNodeLocator(NAME);
    }

    @Override
    public CompType getComponentType() {
        return CompType.ENGINE;
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(final long ts, final Message msg) {
        if (ts > sanityCheckTs)
            sanityCheck();
        newEventIdx = 0;
        msgHandlers.get(msg.getFlow() == MsgFlow.RECV ? msg.getSrcType() : msg.getDstType()).accept(ts, msg);
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * Message handler for application messages.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void applicationMsgHandler(final long ts, final Message msg) {
        // If the message is from the application, check if the message is required to be waiting. If so, wait for the
        // previous request to be finished. GET operations are never blocked.
        if (msg.getFlow() == MsgFlow.RECV && msg.getQType() != QType.GET && isWaitingRequired(msg))
            return;
        applicationMsgHandlerLogic(ts, msg);
    }

    /**
     * Message handler logic for application messages.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void applicationMsgHandlerLogic(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Only REQ message must be received from the application");

                final ObjContent object = msg.getObj();
                Message newMsg = null;
                switch (msg.getQType()) {
                    case GET:
                        profileCounter.queryCounter(QType.GET);

                        // When the Macaron is suspended, go directly to the data lake
                        if (IS_SUSPENDED) {
                            if (ts < ReconfigEvent.getStatWarmupMin() * 60L * 1000000L) {
                                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                        .setDst(MacConf.DATALAKE_NAME, CompType.DATALAKE)));
                                break;
                            } else {
                                IS_SUSPENDED = false;
                            }
                        }

                        // First, check the packing blocks or locked objects in the proxy engine
                        long objSize = isPackingEnabled ? findInPackingBlocks(msg.getObjId())
                                : findInLockedObjects(msg.getObjId());
                        if (objSize > 0L) {
                            profileCounter.cacheCounter(true);
                            addEvent(new Pair<>(ts + lg.get(CompType.ENGINE, CompType.APP, objSize, CompType.DRAM),
                                    msg.setDataExists().setObjSize(objSize).setFlow(MsgFlow.SEND)
                                            .setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME, CompType.APP)
                                            .setMsgType(MsgType.RESP).setDataSrc(CompType.ENGINE)));
                            break;
                        }

                        // If the object is not in the buffer, relay the GET message to the DRAM cache or OSCM
                        addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                .setDst(IS_DRAM_ENABLED ? nodeRoute.getNode() : MacConf.OSCM_NAME,
                                        IS_DRAM_ENABLED ? CompType.DRAM : CompType.OSCM)));
                        break;

                    case PUT:
                        profileCounter.queryCounter(QType.PUT);
                        profileCounter.transfer(MsgFlow.RECV, msg.getObjSize());
                        switch (MacConf.CACHING_POLICY) {
                            case WRITE_THROUGH:
                                // If the Macaron is suspended, go directly to the data lake
                                if (IS_SUSPENDED) {
                                    if (ts < ReconfigEvent.getStatWarmupMin() * 60L * 1000000L) {
                                        putCounter.put(msg.getReqId(), 1); // Datalake
                                        addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                                .setDst(MacConf.DATALAKE_NAME, CompType.DATALAKE)));
                                        break;
                                    } else {
                                        IS_SUSPENDED = false;
                                    }
                                }

                                // Update data in DRAM server, OSCM, and datalake
                                putCounter.put(msg.getReqId(), IS_DRAM_ENABLED ? 2 : 1); // DRAM (if enabled) and Datalake

                                // Update DRAM server
                                if (IS_DRAM_ENABLED) {
                                    addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND)
                                            .setSrc(NAME, CompType.ENGINE).setDst(nodeRoute.getNode(), CompType.DRAM)));
                                }

                                // Update OSCM
                                if (IS_OSC_ENABLED)
                                    admitObjectToOSC(ts, object);

                                // Update data lake
                                newMsg = getFreeMsg();
                                newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE,
                                        MacConf.DATALAKE_NAME, CompType.DATALAKE).setObj(object);
                                addEvent(new Pair<Long, Event>(ts, newMsg));
                                break;

                            default:
                                throw new RuntimeException("Cannot reach here");
                        }
                        break;

                    case DELETE:
                        profileCounter.queryCounter(QType.DELETE);

                        // If the Macaron is suspended, go directly to the data lake
                        if (IS_SUSPENDED) {
                            if (ts < ReconfigEvent.getStatWarmupMin() * 60L * 1000000L) {
                                deleteCounter.put(msg.getReqId(), 1); // Datalake
                                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                        .setDst(MacConf.DATALAKE_NAME, CompType.DATALAKE)));
                                break;
                            } else {
                                IS_SUSPENDED = false;
                            }
                        }

                        // Delete from DRAM server (if exists), OSCM server, OSC (if packing == false), and Datalake
                        deleteCounter.put(msg.getReqId(), IS_DRAM_ENABLED && IS_OSC_ENABLED && !isPackingEnabled ? 4
                                : IS_DRAM_ENABLED && IS_OSC_ENABLED ? 3 : (IS_OSC_ENABLED || IS_DRAM_ENABLED) ? 2 : 1);

                        // If packing is enabled, delete the data from the packing block of this cache engine
                        if (isPackingEnabled)
                            removeFromPacking(object);

                        // Delete from DRAM server
                        if (IS_DRAM_ENABLED) {
                            newMsg = getFreeMsg();
                            newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.DELETE, MsgType.REQ, NAME, CompType.ENGINE,
                                    nodeRoute.getNode(), CompType.DRAM).setObj(object);
                            addEvent(new Pair<Long, Event>(ts, newMsg));
                        }

                        // Delete from OSCM & OSC (when packing is disabled)
                        if (IS_OSC_ENABLED) {
                            addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                    .setDst(MacConf.OSCM_NAME, CompType.OSCM)));
                            if (!isPackingEnabled) {
                                deleteCounter.put(msg.getReqId(), deleteCounter.get(msg.getReqId()) + 1);
                                newMsg = getFreeMsg();
                                newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.DELETE, MsgType.REQ, NAME,
                                        CompType.ENGINE, MacConf.OSC_NAME, CompType.OSC).setObj(object);
                                addEvent(new Pair<Long, Event>(ts, newMsg));
                            }
                        }

                        // Delete from data lake
                        newMsg = getFreeMsg();
                        newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.DELETE, MsgType.REQ, NAME, CompType.ENGINE,
                                MacConf.DATALAKE_NAME, CompType.DATALAKE).setObj(object);
                        addEvent(new Pair<Long, Event>(ts, newMsg));
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Only RESP message must be sent to the application");
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                if (msg.getQType() == QType.GET && msg.getDataExists()) {
                    assert msg.getDataSrc() != null;
                    profileCounter.transfer(MsgFlow.SEND, msg.getObjSize());
                }
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Handles message coming from/going to the DRAM server.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void dramServerMsgHandler(final long ts, final Message msg) {
        if (!IS_DRAM_ENABLED)
            throw new RuntimeException("DRAM server must be enabled");

        Message newMsg;
        int counter;
        switch (msg.getFlow()) {
            case RECV:
                switch (msg.getQType()) {
                    case GET:
                        // If data exists in DRAM server, return the data to Application. Else, ask OSCM server
                        if (msg.getDataExists()) { // GET hit
                            long objSize = msg.getObjSize();
                            profileCounter.transfer(MsgFlow.RECV, objSize);

                            addEvent(new Pair<>(ts + lg.get(CompType.ENGINE, CompType.APP, objSize, CompType.DRAM),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP).setDataSrc(CompType.DRAM)));

                            // If data is received from the previous DRAM server during RECONFIG, store it to the new DRAM server
                            if (state == CacheNodeState.RECONFIG && msg.visitedPrvNode()) {
                                newMsg = getFreeMsg();
                                newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE,
                                        nodeRoute.getNode(), CompType.DRAM).setObj(msg.getObj()).setNoResp();
                                addEvent(new Pair<>(ts, newMsg));
                            }
                        } else { // GET miss
                            if ((state == CacheNodeState.RECONFIG || state == CacheNodeState.NEW_RECONFIG)
                                    && !msg.visitedPrvNode()) { // During reconfig, check previous DRAM server
                                String prvNode = prevNodeRoute.getNode(msg.getObj());
                                // If the previous node is not this node, ask the previous node. Else, ask OSCM server
                                addEvent(new Pair<Long, Event>(ts, msg.setMsgType(MsgType.REQ).setFlow(MsgFlow.SEND)
                                        .setSrc(NAME, CompType.ENGINE)
                                        .setDst(prvNode.equals(NAME) ? (IS_OSC_ENABLED ? MacConf.OSCM_NAME : MacConf.DATALAKE_NAME)
                                                : Controller.getDRAMName(Controller.getCacheEngineIdx(prvNode)),
                                                prvNode.equals(NAME) ? (IS_OSC_ENABLED ? CompType.OSCM : CompType.DATALAKE) : CompType.DRAM)
                                        .setVisitedPrvNode(!prvNode.equals(NAME))));
                            } else { // If OSC is enabled, ask OSCM server. Else, ask data lake
                                addEvent(new Pair<Long, Event>(ts, msg.setMsgType(MsgType.REQ).setFlow(MsgFlow.SEND)
                                        .setSrc(NAME, CompType.ENGINE)
                                        .setDst(IS_OSC_ENABLED ? MacConf.OSCM_NAME : MacConf.DATALAKE_NAME,
                                                IS_OSC_ENABLED ? CompType.OSCM : CompType.DATALAKE)));
                            }
                        }
                        break;

                    case PUT:
                        if (msg.getNoResp()) // Response for Put operation by cache promotion or reconfiguration
                            break;

                        counter = putCounter.getOrDefault(msg.getReqId(), -1);
                        if (counter <= 0)
                            throw new RuntimeException("Put counter must be greater than 0");
                        putCounter.put(msg.getReqId(), counter > 1 ? counter - 1 : null);

                        if (counter == 1) { // All the responses are received
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, msg.getObjSize(), CompType.DRAM),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP)));
                            requestDone(ts, msg);
                        }
                        break;

                    case PREFETCH_PUT:
                        break;

                    case DELETE:
                        if (MacConf.CACHING_POLICY != CachingPolicy.WRITE_THROUGH)
                            throw new RuntimeException("Only WRITE_THROUGH caching policy is supported");

                        counter = deleteCounter.getOrDefault(msg.getReqId(), -1);
                        if (counter <= 0)
                            throw new RuntimeException("Delete counter must be greater than 0");
                        deleteCounter.put(msg.getReqId(), counter > 1 ? counter - 1 : null);

                        if (counter == 1) { // All the responses are received
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, 0L, CompType.DRAM),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP)));
                            requestDone(ts, msg);
                        }
                        break;

                    case TERMINATE:
                        // If DRAM server of this node is cleaned up, send ACK message to Controller */
                        if (msg.getMsgType() != MsgType.RESP)
                            throw new RuntimeException("RESP message must be received from DRAM server for TERMINATE, name: " + NAME);
                        clearProxyIdx();
                        // XXX: add 1us timestamp to ensure that STEADY updates are processed before TERMINATE events
                        addEvent(new Pair<>(ts + 1, msg.setQType(QType.ACK).setFlow(MsgFlow.SEND)
                                .setSrc(NAME, CompType.ENGINE).setDst(MacConf.CONTROLLER_NAME, CompType.CONTROLLER)));
                        break;

                    case CLEAR:
                        // If the clean-up process is done, prefetch the data from OSC to fill up the memory again
                        if (msg.getMsgType() != MsgType.RESP || state != CacheNodeState.STEADY)
                            throw new RuntimeException("Only CLEAR RESP is received from DRAM and state is STEADY, name: " + NAME + ", state: " + state);
                        if (ReconfigEvent.IS_PREFETCH_ENABLED)
                            initPrefetchProcess(ts);
                        break;

                    case PREFETCH:
                        if (msg.getMsgType() != MsgType.RESP)
                            throw new RuntimeException("Only PREFETCH RESP is received from DRAM, name: " + NAME);
                        long prefetchDoneTs = prefetchProcess(ts, msg.getDRAMOccupancy());
                        Logger.getGlobal().info("Prefetching time: " + (prefetchDoneTs - ts) + "us");
                        newMsg = getFreeMsg();
                        if (state == CacheNodeState.NEW_RECONFIG) {
                            addEvent(new Pair<>(prefetchDoneTs, newMsg.set(Message.RESERVED_ID, MsgFlow.SEND, 
                                QType.PREFETCH_DONE, MsgType.RESP, NAME, CompType.ENGINE, MacConf.CONTROLLER_NAME, 
                                CompType.CONTROLLER)));
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                if (msg.getQType() == QType.PUT || msg.getQType() == QType.PREFETCH_PUT)
                    profileCounter.transfer(MsgFlow.SEND, msg.getObjSize());
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Handles message coming from/going to the OSCMServer.
     * 
     * @param newEvents new events that should be registered to the TimelineManger
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void oscmServerMsgHandler(final long ts, final Message msg) {
        if (!IS_OSC_ENABLED)
            throw new RuntimeException("OSCM server must be enabled");

        final int counter;
        switch (msg.getFlow()) {
            case RECV:
                switch (msg.getQType()) {
                    case GET:
                        if (msg.getDataExists()) { // Ask OSC for the data
                            addEvent(new Pair<Long, Event>(ts, msg.setDst(MacConf.OSC_NAME, CompType.OSC)
                                    .setFlow(MsgFlow.SEND).setMsgType(MsgType.REQ).setSrc(NAME, CompType.ENGINE)));
                        } else { // Ask data lake for the data (after checking the buffer again)
                            // Check the block buffer first
                            long objSize = isPackingEnabled ? findInPackingBlocks(msg.getObjId())
                                    : findInLockedObjects(msg.getObjId());
                            if (objSize > 0L) {
                                profileCounter.cacheCounter(true);
                                addEvent(new Pair<>(
                                        ts + lg.get(CompType.ENGINE, CompType.APP, objSize, CompType.DRAM),
                                        msg.setDataExists().setObjSize(objSize).setFlow(MsgFlow.SEND)
                                                .setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME, CompType.APP)
                                                .setMsgType(MsgType.RESP).setDataSrc(CompType.ENGINE)));
                                break;
                            }

                            // If the object is not in the block buffer, send GET message to the data lake
                            if (chkObjInDLMsgList(msg)) { // wait for the previous GET operation's return
                                return;
                            } else { // send GET request to the data lake
                                addEvent(new Pair<Long, Event>(ts, msg.setDst(MacConf.DATALAKE_NAME, CompType.DATALAKE)
                                        .setFlow(MsgFlow.SEND).setMsgType(MsgType.REQ).setSrc(NAME, CompType.ENGINE)));
                            }
                        }
                        break;

                    case PREFETCH_GET:
                        if (msg.getDataExists()) { // If the Prefetch data is in OSC, ask OSC for the data.
                            addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.REQ)
                                    .setSrc(NAME, CompType.ENGINE).setDst(MacConf.OSC_NAME, CompType.OSC)));
                        }
                        break;

                    case PUT:
                        if (!msg.getNoResp())
                            throw new RuntimeException("Only NoResp PUT message must be received from OSCMServer");
                        requestDone(ts, msg);
                        break;

                    case DELETE:
                        counter = deleteCounter.getOrDefault(msg.getReqId(), -1);
                        if (counter <= 0)
                            throw new RuntimeException("Delete counter must be greater than 0");
                        deleteCounter.put(msg.getReqId(), counter > 1 ? counter - 1 : null);

                        if (counter == 1) { // All the responses are received
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, 0L, CompType.OSC),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                            .setDst(MacConf.APP_NAME, CompType.APP)));
                            requestDone(ts, msg);
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                assert msg.getMsgType() == MsgType.REQ;
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                assert false;
        }
    }

    /**
     * Handles message coming from/going to the OSC.
     * 
     * @param newEvents new events that should be registered to the TimelineManger
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void oscMsgHandler(final long ts, final Message msg) {
        assert IS_OSC_ENABLED;
        Message newMsg = null;
        switch (msg.getFlow()) {
            case RECV:
                switch (msg.getQType()) {
                    case GET:
                        final long objSize = msg.getObjSize();
                        if (objSize > 0L) { // If the object exists in OSC, send it to the application
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, objSize, CompType.OSC),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP).setDataSrc(CompType.OSC)));
                            profileCounter.transfer(MsgFlow.RECV, objSize);

                            if (IS_DRAM_ENABLED) { // Promote this data to DRAM server
                                newMsg = getFreeMsg();
                                newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE,
                                        nodeRoute.getNode(), CompType.DRAM).setNoResp().setObj(msg.getObj());
                                addEvent(new Pair<Long, Event>(ts, newMsg));
                            }
                        } else { // If the object is GC'ed asynchrnously, data might exist in the data lake.
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.DATALAKE, objSize, CompType.DATALAKE),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                            .setDst(MacConf.DATALAKE_NAME, CompType.DATALAKE).setDataNoExists()));
                        }
                        break;

                    case PREFETCH_GET:
                        profileCounter.transfer(MsgFlow.RECV, msg.getObjSize());
                        addEvent(new Pair<>(ts,
                                msg.setFlow(MsgFlow.SEND).setQType(QType.PREFETCH_PUT).setMsgType(MsgType.REQ)
                                        .setSrc(NAME, CompType.ENGINE).setDst(nodeRoute.getNode(), CompType.DRAM)));
                        break;

                    case PUT:
                        int blockId = isPackingEnabled ? msg.getBlockId() : -1;
                        Map<Integer, Long> block = isPackingEnabled ? lockedBlocks.get(blockId) : null;
                        addEvent(new Pair<Long, Event>(ts,
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.REQ).setSrc(NAME, CompType.ENGINE)
                                        .setDst(MacConf.OSCM_NAME, CompType.OSCM)
                                        .setMdata(isPackingEnabled ? new MetadataContent(blockId, block) : null)));
                        break;

                    case DELETE:
                        assert MacConf.CACHING_POLICY == CachingPolicy.WRITE_THROUGH;
                        QType qType = QType.DELETE;
                        int counter = deleteCounter.getOrDefault(msg.getReqId(), -1);
                        assert counter > 0 : "Counter must be greater than 0";
                        if (counter > 1)
                            deleteCounter.put(msg.getReqId(), counter - 1);
                        else
                            deleteCounter.remove(msg.getReqId());

                        if (counter == 1) { /* All the required resps for delete/write-around are received. */
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, 0L, CompType.OSC),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                            .setDst(MacConf.APP_NAME, CompType.APP).setQType(qType)));
                            requestDone(ts, msg); /* End of this request process */
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                assert msg.getMsgType() == MsgType.REQ;
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                if (msg.getQType() == QType.PUT)
                    profileCounter.transfer(MsgFlow.SEND, isPackingEnabled ? msg.getBlockSize() : msg.getObjSize());
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Handles message coming from/going to the MacaronMaster.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void controllerMsgHandler(final long ts, final Message msg) {
        Message newMsg;
        switch (msg.getFlow()) {
            case RECV:
                switch (msg.getQType()) {
                    case RECONFIG:
                    case NEW_RECONFIG: // (NEW) RECONFIG state begins. Use prev nodeRoute for the passive data movement
                        state = msg.getQType() == QType.RECONFIG ? CacheNodeState.RECONFIG
                                : CacheNodeState.NEW_RECONFIG;
                        prevCacheEngineNames = msg.getCdata().getCacheEngineNameList();
                        prevNodeRoute = new KetamaNodeLocator(prevCacheEngineNames);
                        newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.ACK, MsgType.RESP, NAME,
                                CompType.ENGINE, MacConf.CONTROLLER_NAME, CompType.CONTROLLER);
                        addEvent(new Pair<>(ts, newMsg));
                        break;

                    case STEADY: // End of RECONFIG state
                        state = CacheNodeState.STEADY;
                        prevCacheEngineNames = null;
                        prevNodeRoute = null;
                        break;

                    case TERMINATE: // Delete all the objects stored in the cache corresponding to this machine
                        addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                .setDst(nodeRoute.getNode(), CompType.DRAM)));
                        break;

                    case PREFETCH: // Save information that will be used for the prefetching
                        if (!ReconfigEvent.IS_PREFETCH_ENABLED)
                            throw new RuntimeException("Prefetching must be enabled");

                        scanOrder = msg.getScanOrder();
                        scanOrderSize = msg.getScanOrderSize();
                        scanOrderNode = msg.getScanOrderNode();
                        scanOrderCount = msg.getScanOrderCount();
                        if (state == CacheNodeState.NEW_RECONFIG)
                            initPrefetchProcess(ts);
                        break;

                    case CLEAR: // Clear out DRAM data that are not coresponding to this server anymore
                        clearCreatingBlock();
                        cleanProxyIdx(msg.getCdata().getCacheEngineNameList());
                        addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE)
                                .setDst(nodeRoute.getNode(), CompType.DRAM)));
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                assert msg.getQType() == QType.ACK || msg.getQType() == QType.PREFETCH_DONE;
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Handles message coming from/going to the Datalake.
     * 
     * @param ts timestamp when this message should be triggered
     * @param msg new message coming from/going to the Application
     */
    private void datalakeMsgHandler(final long ts, final Message msg) {
        Message newMsg = null;
        int counter;
        switch (msg.getFlow()) {
            case RECV:
                switch (msg.getQType()) {
                    case GET:
                        if (msg.getDataExists()) { // Data exists in the data lake
                            // Send the data to the Application.
                            long objSize = msg.getObjSize();
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, objSize, CompType.DATALAKE),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP).setDataSrc(CompType.DATALAKE)));
                            profileCounter.transfer(MsgFlow.RECV, objSize);

                            // If the Macaron is suspended, just get back to the Applicaiton and return
                            if (IS_SUSPENDED) {
                                if (ts < ReconfigEvent.getStatWarmupMin() * 60L * 1000000L) {
                                    resumeMsgsInDLMsgList(ts, msg);
                                    break;
                                } else {
                                    IS_SUSPENDED = false;
                                }
                            }

                            // Admit data to DRAM
                            ObjContent obj = msg.getObj();
                            if (IS_DRAM_ENABLED) {
                                newMsg = getFreeMsg();
                                newMsg.set(msg.getReqId(), MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE,
                                        nodeRoute.getNode(), CompType.DRAM).setObj(obj).setNoResp();
                                addEvent(new Pair<Long, Event>(ts, newMsg));
                            }

                            // Admit data to OSC (if OSC is enabled)
                            if (IS_OSC_ENABLED)
                                admitObjectToOSC(ts, obj);
                        } else { // If data does not exist in the data lake
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, 0L, CompType.DATALAKE),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP)));
                        }

                        // resume those blocked data lake GET request messages
                        resumeMsgsInDLMsgList(ts, msg);
                        break;

                    case PUT:
                        counter = putCounter.getOrDefault(msg.getReqId(), -1);
                        if (counter <= 0)
                            throw new RuntimeException("Counter must be greater than 0");
                        putCounter.put(msg.getReqId(), counter > 1 ? counter - 1 : null);

                        if (counter == 1) { // All the responses are received
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, msg.getObjSize(), CompType.DATALAKE),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP)));
                            requestDone(ts, msg);
                        }
                        break;

                    case DELETE:
                        counter = deleteCounter.getOrDefault(msg.getReqId(), -1);
                        if (counter <= 0)
                            throw new RuntimeException("Delete counter must be greater than 0");
                        deleteCounter.put(msg.getReqId(), counter > 1 ? counter - 1 : null);

                        if (counter == 1) { // All the responses are received
                            addEvent(new Pair<Long, Event>(
                                    ts + lg.get(CompType.ENGINE, CompType.APP, 0L, CompType.DATALAKE),
                                    msg.setFlow(MsgFlow.SEND).setSrc(NAME, CompType.ENGINE).setDst(MacConf.APP_NAME,
                                            CompType.APP)));
                            requestDone(ts, msg);
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                assert msg.getMsgType() == MsgType.REQ;
                if (msg.getQType() == QType.PUT)
                    profileCounter.transfer(MsgFlow.SEND, msg.getObjSize());
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Clear the block buffer for the current cache engine.
     */
    private void clearCreatingBlock() {
        for (Map<Integer, Long> block : creatingBlocks.values()) {
            List<Integer> objIDs = block.keySet().stream().collect(Collectors.toList());
            for (int objID : objIDs)
                removeFromPacking(new ObjContent(objID));
        }
    }

    /**
     * Admit the object to OSC. Add the object to the packing block if packing is enabled. If the packing block is full,
     * send the block to OSC. If packing is disabled, send the object to OSC.
     * 
     * @param ts timestamp when this object is admitted to OSC
     * @param object object that should be admitted to OSC
     */
    private void admitObjectToOSC(long ts, ObjContent object) {
        Message newMsg = null;
        if (isPackingEnabled) {
            int packedBlockId = addToPacking(object);
            if (packedBlockId != -1 && tryLockAcquireForBlock(packedBlockId)) {
                newMsg = lockAndSendBlockToOSC(packedBlockId);
            }
        } else {
            if (tryLockAcquireForObject(object))
                newMsg = lockAndSendObjectToOSC(object.getObjId());
        }
        if (newMsg != null)
            addEvent(new Pair<Long, Event>(ts, newMsg));
    }

    /**
     * [Only used when packing == true] Add the new object to the packing block. If the new object size is greater than
     * the {@value #MAX_PACKING_SIZE}, generate a block with the single object and acquire a lock for the block.
     * 
     * @param object new object that should be added to the packing object
     * @return block id for the block that is finished its packing. if the block is not ready, return -1.
     */
    private int addToPacking(final ObjContent object) {
        int lockWaitBlockId = -1;

        final long objSize = object.getObjSize();
        final int objectId = object.getObjId();
        final int pos = getOrAssignOidToPos(objectId);
        objIdToBlockInfos[objectId][pos][1] = objSize;

        // If the new object size is greater than the packing block size, generate a block with the single object
        if (objSize > MAX_PACKING_SIZE) {
            // Remove this item from creatingBlocks (if there is any)
            Map<Integer, Long> block = creatingBlocks.get(globalBlockId);
            if (block != null) {
                long oldObjSize = block.getOrDefault(objectId, -1L);
                if (oldObjSize != -1L) {
                    currPackingCnt--;
                    currPackingSize -= oldObjSize;
                    block.remove(objectId);
                    objIdToBlockInfos[objectId][pos][0] -= 1L;
                }
            }

            lockWaitBlockId = OSCMServer.genBlockId();
            block = getFreeBlock();
            block.put(objectId, objSize);
            lockWaitBlocks.put(lockWaitBlockId, block);
            objIdToBlockInfos[objectId][pos][0] += 1L;

            return lockWaitBlockId;
        }

        // If the current packing block is already full, spill the current packing block to OSC
        if (currPackingCnt + 1 > maxPackingCnt || currPackingSize + objSize > MAX_PACKING_SIZE) {
            lockWaitBlocks.put(globalBlockId, creatingBlocks.remove(globalBlockId));
            lockWaitBlockId = globalBlockId;

            globalBlockId = OSCMServer.genBlockId();
            currPackingCnt = 0;
            currPackingSize = 0L;
        }

        Map<Integer, Long> block = creatingBlocks.get(globalBlockId);
        if (block == null) {
            block = getFreeBlock();
            creatingBlocks.put(globalBlockId, block);
        }
        long oldObjSize = block.getOrDefault(objectId, -1L);
        if (oldObjSize == -1L) {
            currPackingCnt++;
            currPackingSize += objSize;
            objIdToBlockInfos[objectId][pos][0] += 1L;
        } else {
            currPackingSize += (objSize - oldObjSize);
        }
        block.put(objectId, objSize);

        return lockWaitBlockId;
    }

    /**
     * Check if the requested object is in the packing blocks
     * 
     * @param objectId object id to check if it is in the local packing blocks
     * @return object size if exits, else 0L
     */
    private long findInPackingBlocks(int objectId) {
        final int pos = getOrAssignOidToPos(objectId);
        return objIdToBlockInfos[objectId][pos][0] == 0L ? 0L : objIdToBlockInfos[objectId][pos][1];
    }

    /**
     * Check if the requested object is in the locked objects
     * @param objectId object id to check if it is in the local locked objects
     * @return object size if exits, else 0L
     */
    private long findInLockedObjects(int objectId) {
        ObjContent obj = lockWaitObjects.getOrDefault(objectId, null);
        if (obj != null)
            return obj.getObjSize();
        obj = lockedObjects.getOrDefault(objectId, null);
        return obj == null ? 0L : obj.getObjSize();
    }

    /**
     * Check if there is on-the-fly requests to the data lake for the corresponding object.
     * 
     * @param msg message that requests data from the data lake
     * @return whether there is on-the-fly requests
     */
    private boolean chkObjInDLMsgList(Message msg) {
        int objectId = msg.getObjId();
        int pos = getOrAssignOidToPos(objectId);
        List<Message> msgList = objToDLGetWaitMsgLists[objectId][pos];
        msgList.add(msg);
        return msgList.size() > 1;
    }

    /**
     * Resume messages that were blocked by the previous remote data lake GET operation
     * 
     * @param ts current timestamp
     * @param msg response message received from the data lake
     */
    private void resumeMsgsInDLMsgList(long ts, Message msg) {
        int objectId = msg.getObjId();
        int pos = getOrAssignOidToPos(objectId);
        List<Message> msgList = objToDLGetWaitMsgLists[objectId][pos];
        for (Message rMsg : msgList)
            if (rMsg.getReqId() != msg.getReqId())
                addEvent(new Pair<Long, Event>(ts, rMsg));
        msgList.clear();
    }

    private void addEvent(Pair<Long, Event> event) {
        newEvents[newEventIdx++] = event;
    }

    /**
     * Prepare {@code #readyMsgs} for the next {@code #READY_MSG_SIZE} messages.
     */
    private void prepareReadyMsgs() {
        this.readyMsgs = new Message[READY_MSG_SIZE];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < READY_MSG_SIZE; i += threadLength)
                readyMsgs[i] = new Message();
        });
        readyMsgIdx = 0;
    }

    /**
     * Get a free message from {@code #readyMsgs}. If there is no free message, prepare {@code #readyMsgs} for the next
     * {@code #READY_MSG_SIZE} messages.
     * 
     * @return a free message
     */
    private Message getFreeMsg() {
        if (readyMsgIdx == READY_MSG_SIZE)
            prepareReadyMsgs();
        return readyMsgs[readyMsgIdx++];
    }

    /**
     * Get a free block from {@code #freeBlocks}. If there is no free block, prepare {@code #freeBlocks} additional
     * {@code #freeBlockIncr} blocks.
     * 
     * @return a free block
     */
    private Map<Integer, Long> getFreeBlock() {
        if (freeBlocks.isEmpty())
            prepareFreeBlocks();
        return freeBlocks.remove();
    }

    /**
     * Prepare {@code #freeBlocks} additional {@code #freeBlockIncr} blocks.
     */
    private void prepareFreeBlocks() {
        for (int i = 0; i < freeBlockIncr; i++)
            freeBlocks.add(new HashMap<>());
    }

    /**
     * Return the used block to {@code #freeBlocks}.
     * 
     * @param block block that should be returned to {@code #freeBlocks}
     */
    private void returnFreeBlock(Map<Integer, Long> block) {
        block.clear();
        freeBlocks.add(block);
    }

    /**
     * Delete the object from the packing block (if exists)
     * 
     * @param object object that should be deleted from the packing block
     */
    private void removeFromPacking(final ObjContent object) {
        final int objectId = object.getObjId();
        final int pos = getOrAssignOidToPos(objectId);
        Map<Integer, Long> block = creatingBlocks.get(globalBlockId);
        if (block == null)
            return;

        long objectSize = block.getOrDefault(objectId, -1L);
        if (objectSize == -1L)
            return;

        objIdToBlockInfos[objectId][pos][0] -= 1L;
        block.remove(objectId);
        currPackingCnt--;
        currPackingSize -= objectSize;
    }

    /**
     * Check if the message is necessary to wait in the waitingQueue. If so, insert the message to the waitingQueue and
     * return true. Else, return false.
     * 
     * @param msg message that should be checked if it is necessary to wait in the waitingQueue
     * @return whether the message is necessary to wait in the waitingQueue
     */
    private boolean isWaitingRequired(final Message msg) {
        if (msg.getFlow() != MsgFlow.RECV || msg.getMsgType() != MsgType.REQ || msg.getQType() == QType.GET)
            throw new RuntimeException("Only non-GET REQ message can be inserted into the waitingQueue");

        int objectId = msg.getObjId();
        int pos = getOrAssignOidToPos(objectId);

        // If there are waiting requests, insert the new request to the waitingQueue. 
        if (waitingQueueLengths[objectId][pos] > 0) {
            addNewMsgToWaitingQueue(msg);
            return true;
        }

        // If there are no running requests, insert the new request to the running request
        if (runningReqCounts[objectId][pos] == 0) {
            runningReqCounts[objectId][pos]++;
            runningQueryTypes[objectId][pos] = msg.getQType();
            return false;
        }

        // If there are running requests, wait in the waiting queue
        addNewMsgToWaitingQueue(msg);
        return true;
    }

    /**
     * Add new message to the corresponding waitingQueue at the end of the queue. 
     * 
     * @param msg message that will be inserted to the waitingQueue
     */
    private void addNewMsgToWaitingQueue(Message msg) {
        int objectId = msg.getObjId();
        int pos = getOrAssignOidToPos(objectId);
        extendWaitingQueueIfNeeded(objectId);
        int endIdx = waitingQueueEnds[objectId][pos];
        waitingQueues[objectId][pos][endIdx] = msg;
        waitingQueueEnds[objectId][pos] = (endIdx + 1) % waitingQueues[objectId][pos].length;
        waitingQueueLengths[objectId][pos]++;
    }

    /**
     * Add new message to the corresponding waitingQueue at the front of the queue.
     * 
     * @param objectId object id of the message
     * @param msg message that will be inserted to the waitingQueue
     */
    private void addNewMsgToWaitingQueueAtFront(int objectId, Message msg) {
        int pos = getOrAssignOidToPos(objectId);
        extendWaitingQueueIfNeeded(objectId);
        int begIdx = waitingQueueBegs[objectId][pos];
        begIdx = begIdx - 1 < 0 ? waitingQueues[objectId][pos].length - 1 : begIdx - 1;

        if (waitingQueueLengths[objectId][pos] == 0) {
            // If the waitingQueue is empty, insert the message to the front of the queue
            waitingQueues[objectId][pos][begIdx] = msg;
        } else {
            // Find the first message that its query type is not LOCK_ACQUIRE. If found, the new LOCK_ACUIRE message 
            // will be positioned in that position. While searching for the position, push all existing messages one
            // position front.
            boolean isInserted = false;
            int endIdx = waitingQueueEnds[objectId][pos];
            endIdx = endIdx - 1 < 0 ? waitingQueues[objectId][pos].length - 1 : endIdx - 1;
            for (int i = begIdx; i != endIdx; i = (i + 1) % waitingQueues[objectId][pos].length) {
                int nxtIdx = (i + 1) % waitingQueues[objectId][pos].length;
                if (waitingQueues[objectId][pos][nxtIdx].getQType() != QType.LOCK_ACQUIRE) {
                    waitingQueues[objectId][pos][i] = msg;
                    isInserted = true;
                    break;
                } else {
                    waitingQueues[objectId][pos][i] = waitingQueues[objectId][pos][nxtIdx];
                }
            }
            if (!isInserted)
                waitingQueues[objectId][pos][endIdx] = msg;
        }
        waitingQueueBegs[objectId][pos] = begIdx;
        waitingQueueLengths[objectId][pos]++;
    }

    /**
     * Pop the first message from the waitingQueue.
     * 
     * @param objectId object id of the message that will be popped from the waitingQueue
     * @return the first message from the waitingQueue
     */
    private Message popMsgFromWaitingQueue(int objectId) {
        int pos = getOrAssignOidToPos(objectId);
        if (waitingQueueLengths[objectId][pos] == 0)
            throw new RuntimeException("Cannot pop message from empty waitingQueue");
        int begIdx = waitingQueueBegs[objectId][pos];
        Message retMsg = waitingQueues[objectId][pos][begIdx];
        waitingQueueBegs[objectId][pos] = (begIdx + 1) % waitingQueues[objectId][pos].length;
        waitingQueueLengths[objectId][pos]--;
        return retMsg;
    }

    /**
     * Extend the waiting queue size (2x) if the queue is full.
     * 
     * @param objectId object id of the message that will lengthen the waitingQueue
     */
    private void extendWaitingQueueIfNeeded(int objectId) {
        int pos = getOrAssignOidToPos(objectId);
        int waitingQueueSize = waitingQueues[objectId][pos].length;
        if (waitingQueueLengths[objectId][pos] == waitingQueueSize) {
            int begIdx = waitingQueueBegs[objectId][pos];
            int newWaitingQueueSize = waitingQueueSize * 2;
            writeLineToFile(queueExtendDebugFilename, objectId + "," + waitingQueueSize + "," + newWaitingQueueSize,
                    false);

            Message[] newWaitingQueue = new Message[newWaitingQueueSize];

            System.arraycopy(waitingQueues[objectId][pos], begIdx, newWaitingQueue, 0, waitingQueueSize - begIdx);
            System.arraycopy(waitingQueues[objectId][pos], 0, newWaitingQueue, waitingQueueSize - begIdx, begIdx);

            waitingQueues[objectId][pos] = newWaitingQueue;
            waitingQueueBegs[objectId][pos] = 0;
            waitingQueueEnds[objectId][pos] = waitingQueueSize;
        }
    }

    /**
     * All the end-to-end process of the request is done. Check both waitingQueue and running queue, so that if there 
     * are some blocked requests by this request, insert the blocked requests to the runnig queue.
     * 
     * @param ts current timestamp when all of this message-related process is done.
     * @param msg message that will be dequeued from the running queue.
     */
    private void requestDone(final long ts, final Message msg) {
        if (msg.getReqId() == Message.RESERVED_ID) { // Put request for OSC/OSCM is done
            if (isPackingEnabled) {
                int blockId = msg.getBlockId();
                Map<Integer, Long> block = lockedBlocks.remove(blockId);
                for (int objectId : block.keySet()) {
                    int pos = getOrAssignOidToPos(objectId);
                    objIdToBlockInfos[objectId][pos][0] -= 1L;
                    if (objIdToBlockInfos[objectId][pos][0] < 0L)
                        throw new RuntimeException("objIdToBlockInfos[objectId][pos][0] >= 0");
                    requestDone(ts, objectId);
                }
                returnFreeBlock(block);
            } else {
                lockedObjects.remove(msg.getObjId());
                requestDone(ts, msg.getObjId());
            }
        } else {
            requestDone(ts, msg.getObjId());
        }
    }

    /**
     * If there are some blocked requests by this request, insert the blocked requests to the runnig queue.
     * 
     * @param ts current timestamp when all of this message-related process is done.
     * @param objectId object id of the message that will be dequeued from the running queue.
     */
    private void requestDone(final long ts, final int objectId) {
        final int pos = getOrAssignOidToPos(objectId);
        if (--runningReqCounts[objectId][pos] < 0)
            throw new RuntimeException("Running request count for the object " + objectId + " must be greater than 0");

        // If there is no waiting requests, nothing to do further
        if (waitingQueueLengths[objectId][pos] == 0) {
            runningQueryTypes[objectId][pos] = QType.NONE;
            return;
        }

        // If there is a waiting request, pop the first request from the waitingQueue and insert it to the running queue
        Message firstWaitingMsg = popMsgFromWaitingQueue(objectId);
        runningReqCounts[objectId][pos]++;
        runningQueryTypes[objectId][pos] = firstWaitingMsg.getQType();

        if (firstWaitingMsg.getQType() == QType.PUT || firstWaitingMsg.getQType() == QType.DELETE) {
            applicationMsgHandlerLogic(ts, firstWaitingMsg);
            return;
        }

        if (firstWaitingMsg.getQType() == QType.LOCK_ACQUIRE) {
            if (isPackingEnabled) {
                int blockId = firstWaitingMsg.getBlockId();
                lockAcquireMsgs.add(firstWaitingMsg.resetBlockId()); // return the lock acquiring msg to the pool
                if (--lockAcquiring[blockId] == 0) { // all the objects are succeeded to acquire lock
                    Message newMsg = lockAndSendBlockToOSC(blockId);
                    if (newMsg == null)
                        throw new RuntimeException("newMsg must not be null in this logic");
                    addEvent(new Pair<Long, Event>(ts, newMsg));
                }
                if (lockAcquiring[blockId] < 0)
                    throw new RuntimeException("lockAcquiring[blockId] must be greater than 0");
            } else {
                lockAcquireMsgs.add(firstWaitingMsg); // return the lock acquiring msg to the pool
                Message newMsg = lockAndSendObjectToOSC(objectId);
                assert newMsg != null;
                addEvent(new Pair<Long, Event>(ts, newMsg));
            }
            return;
        }

        throw new RuntimeException("Cannot reach here");
    }

    /**
     * This function is called when the block is successfully locked and ready to be sent to OSC/OSCM.
     * 
     * @param blockId block id of the block that will be stored in the OSC
     * @return new message sending data to OSC
     */
    private Message lockAndSendBlockToOSC(int blockId) {
        Map<Integer, Long> block = lockWaitBlocks.remove(blockId);
        if (block.isEmpty())
            return null;

        lockedBlocks.put(blockId, block);
        long blockSize = block.values().stream().mapToLong(Long::longValue).sum();

        Message newMsg = getFreeMsg();
        newMsg.set(Message.RESERVED_ID, MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE, MacConf.OSC_NAME,
                CompType.OSC).setNoResp().setBlockSize(blockSize).setBlockId(blockId);
        return newMsg;
    }

    /**
     * This function is called when the object is successfully locked and ready to be sent to OSC/OSCM.
     * 
     * @param objectId object id of the object that will be stored in the OSC
     * @return new message sending data to OSC
     */
    private Message lockAndSendObjectToOSC(int objectId) {
        ObjContent object = lockWaitObjects.get(objectId);
        int waitCount = lockWaitCounts.get(objectId);
        if (waitCount > 1) {
            lockWaitCounts.put(objectId, waitCount - 1);
        } else {
            lockWaitObjects.remove(objectId);
            lockWaitCounts.remove(objectId);
        }
        if (object == null || lockedObjects.putIfAbsent(objectId, object) != null)
            throw new RuntimeException("Object is not null and must not be in the lockedObjects");

        Message newMsg = getFreeMsg();
        newMsg.set(Message.RESERVED_ID, MsgFlow.SEND, QType.PUT, MsgType.REQ, NAME, CompType.ENGINE, MacConf.OSC_NAME,
                CompType.OSC).setNoResp().setObj(object);
        return newMsg;
    }

    /**
     * Try acquire lock for all the objects in the given block.
     * 
     * @param blockId block id for the block to be locked
     * @return check if the block is successfully locked or not
     */
    private boolean tryLockAcquireForBlock(int blockId) {
        if (lockAcquiring[blockId] != 0)
            throw new RuntimeException("This block is already waiting for the lock");
        Map<Integer, Long> block = lockWaitBlocks.get(blockId);
        for (int objectId : block.keySet()) /* For each object, try to acquire a lock */
            tryLockAcquireForBlockItem(objectId, blockId);
        return lockAcquiring[blockId] == 0;
    }

    /**
     * Add {@code #lockAcquireMsgIncr} LOCK_ACQUIRE messages to {@code #lockAcquireMsgs}.
     */
    private void addLockAcquireMsgsIfNeeded() {
        if (lockAcquireMsgs.isEmpty())
            for (int i = 0; i < lockAcquireMsgIncr; i++)
                lockAcquireMsgs
                        .add(new Message(Message.RESERVED_ID, null, QType.LOCK_ACQUIRE, null, null, null, null, null));
    }

    /**
     * Try to acquire a lock for the given object. If the lock is successfully acquired, return. Else, add the message
     * to {@code #waitingQueues} and indicate that in {@code #lockAcquiring} array.
     * 
     * @param objectId corresponding object id that needs lock
     * @param blockId corresponding block id to be blocked
     */
    private void tryLockAcquireForBlockItem(int objectId, int blockId) {
        final int pos = getOrAssignOidToPos(objectId);
        int runningReqCount = runningReqCounts[objectId][pos];

        if (runningReqCount == 0) {
            runningReqCounts[objectId][pos]++;
            runningQueryTypes[objectId][pos] = QType.LOCK_ACQUIRE;
        } else {
            addLockAcquireMsgsIfNeeded();
            Message lockMsg = lockAcquireMsgs.remove();
            lockMsg.setBlockId(blockId);
            addNewMsgToWaitingQueueAtFront(objectId, lockMsg);
            lockAcquiring[blockId]++;
        }
    }

    /**
     * Try to acquire a lock for the given object. If the lock is successfully acquired, return true. Else, add the
     * message to {@code #waitingQueues} and return false.
     * 
     * @param object corresponding object that needs lock
     * @return whether the lock is successfully acquired or not
     */
    private boolean tryLockAcquireForObject(ObjContent object) {
        final int objectId = object.getObjId();
        final int pos = getOrAssignOidToPos(objectId);
        int runningReqCount = runningReqCounts[objectId][pos];

        ObjContent existObj = lockWaitObjects.putIfAbsent(objectId, object);
        lockWaitCounts.put(objectId, existObj == null ? 1 : lockWaitCounts.get(objectId) + 1);

        if (runningReqCount == 0) {
            runningReqCounts[objectId][pos]++;
            runningQueryTypes[objectId][pos] = QType.LOCK_ACQUIRE;
            return true;
        } else {
            addLockAcquireMsgsIfNeeded();
            Message lockMsg = lockAcquireMsgs.remove();
            addNewMsgToWaitingQueueAtFront(objectId, lockMsg);
            return false;
        }
    }

    /* Start of prefetching related functions. */

    /**
     * Initialize the prefetching process. Ask the corresponding DRAM server about memory occupancy information.
     * 
     * @param ts tiemstamp that the prefetching process is initalized.
     */
    private void initPrefetchProcess(long ts) {
        if (scanOrder == null || scanOrderNode == null)
            throw new RuntimeException("scanOrder and scanOrderNode must not be null, name: " + NAME);

        Message msg = getFreeMsg();
        msg.set(Message.RESERVED_ID, MsgFlow.SEND, QType.PREFETCH, MsgType.REQ, NAME, CompType.ENGINE,
                nodeRoute.getNode(), CompType.DRAM);
        addEvent(new Pair<>(ts, msg));
    }

    /**
     * Fill in 95% of the DRAM server's memory by prefetching 95% of data. Send all the GET requests to OSCM/OSC for the
     * prefetching and after the expected time of prefetching, register a PREFETCH_DONE message that will be sent to the
     * Controller.
     * 
     * @param ts timestamp when the GET requests are started to be sent for the prefetching
     * @param cacheOccupancy current DRAM server's memory occupancy
     * @return expected timestamp that the PREFETCH_DONE request will be sent to the Controller
     */
    private long prefetchProcess(long ts, Pair<Long, Long> cacheOccupancy) {
        long cacheOccupiedSize = cacheOccupancy.getFirst();
        long cacheSize = cacheOccupancy.getSecond();
        int cacheSizeGB = (int) (cacheSize / 1024L / 1024L / 1024L);
        long prefetchDataSize = (long) (cacheSize * 0.95) - cacheOccupiedSize;

        // If the DRAM is almost full, don't prefetch
        if (prefetchDataSize <= 0L) {
            return ts + 1000000L;
        }

        long size = 0L;
        int prefetchIdx = 0, lastPrefetchIdx = 0;
        while (size < prefetchDataSize && prefetchIdx < scanOrderCount) {
            if (scanOrderNode[prefetchIdx] == engineId) {
                size += scanOrderSize[prefetchIdx];
                lastPrefetchIdx = prefetchIdx;
            }
            prefetchIdx++;
        }

        int sent = 0;
        prefetchIdx = lastPrefetchIdx;
        while (prefetchIdx >= 0) {
            if (scanOrderNode[prefetchIdx] == engineId) {
                Message msg = getFreeMsg();
                msg.set(Message.RESERVED_ID, MsgFlow.SEND, QType.PREFETCH_GET, MsgType.REQ, NAME, CompType.ENGINE,
                        MacConf.OSCM_NAME, CompType.OSCM).setObj(new ObjContent(scanOrder[prefetchIdx]));
                addEvent(new Pair<>(ts, msg));
                if (++sent % (PREFETCH_CNT_PER_SEC_GB * cacheSizeGB) == 0)
                    ts += 1000000L; /* per second */
            }
            prefetchIdx--;
        }
        ts += 1000000L;
        return ts;
    }
    /* End of prefetch related functions. */

    /**
     * Sanity check for the cache engine.
     */
    private void sanityCheck() {
        sanityCheckTs += sanityCheckInterval;

        // Count the number of all the objects in the cache engine's blocks
        int numCPUs = Runtime.getRuntime().availableProcessors();
        Thread threads[] = new Thread[numCPUs];
        int[] cnts = new int[numCPUs];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = threadId; j < sharedVarSize; j += threads.length) {
                    int pos = getAssignedOidToPos(j);
                    if (pos != -1)
                        cnts[threadId] += (int) objIdToBlockInfos[j][pos][0];
                }
            });
            threads[i].start();
        }

        try {
            for (int i = 0; i < threads.length; i++)
                threads[i].join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int count1 = Arrays.stream(cnts).sum();
        int count2 = creatingBlocks.values().stream().mapToInt(Map::size).sum()
                + lockWaitBlocks.values().stream().mapToInt(Map::size).sum()
                + lockedBlocks.values().stream().mapToInt(Map::size).sum();

        if (count1 != count2) {
            Logger.getGlobal().info("Creating blocks: " + creatingBlocks.toString());
            Logger.getGlobal().info("Lock waiting blocks: " + lockWaitBlocks.toString());
            Logger.getGlobal().info("Locked blocks: " + lockedBlocks.toString());
            throw new RuntimeException("Sanity check failed: " + count1 + " != " + count2 + ". Name: " + NAME);
        }
    }

    @Override
    public ProfileInfo getAndResetProfileInfo() {
        return profileCounter.getAndResetProfileInfo();
    }

    @Override
    public void saveProfileInfo(long timestamp, CompType componentType, String name, ProfileInfo profileInfo) {
        throw new RuntimeException("Cannot reach here");
    }

    /**
     * Write a new line to the file
     * 
     * @param filename file name to write
     * @param line line to write
     * @param newFile whether to create a new file or not
     */
    private static void writeLineToFile(String filename, String line, boolean newFile) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename, !newFile))) {
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Save debug files for the cache engine. If {@code #DEBUG} is false, this function does nothing.
     */
    public void saveDebugFiles() {
        if (DEBUG) {
            Logger.getGlobal().info("Start saving debug files");

            // Save queue debug file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(queueDebugFilename, true))) {
                for (int objectId = 0; objectId < sharedVarSize; objectId++) {
                    int pos = getAssignedOidToPos(objectId);
                    if (pos == -1)
                        continue;

                    if (runningReqCounts[objectId][pos] > 0)
                        bw.write(objectId + ",Running,NaN," + runningQueryTypes[objectId][pos] + "\n");

                    for (int i = 0; i < waitingQueueLengths[objectId][pos]; i++) {
                        int idx = (waitingQueueBegs[objectId][pos] + i) % waitingQueues[objectId][pos].length;
                        Message msg = waitingQueues[objectId][pos][idx];
                        bw.write(objectId + ",Waiting," + msg.getReqId() + "," + msg.getQType() + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (isPackingEnabled) {
            // Save lock debug file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(lockDebugFilename, true))) {
                for (int i = 0; i < lockAcquiring.length; i++) {
                    if (lockAcquiring[i] > 0)
                        bw.write(i + "," + lockAcquiring[i] + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Save block information debug file
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(blockInfoFilename, true))) {
                int[] blockIds = creatingBlocks.keySet().stream().mapToInt(Integer::intValue).toArray();
                Arrays.sort(blockIds);
                for (int blockId : blockIds) {
                    Map<Integer, Long> block = creatingBlocks.get(blockId);
                    for (int objectId : block.keySet())
                        bw.write("Creating," + blockId + "," + objectId + "," + block.get(objectId) + "\n");
                }

                blockIds = lockWaitBlocks.keySet().stream().mapToInt(Integer::intValue).toArray();
                Arrays.sort(blockIds);
                for (int blockId : blockIds) {
                    Map<Integer, Long> block = lockWaitBlocks.get(blockId);
                    for (int objectId : block.keySet())
                        bw.write("LockWait," + blockId + "," + objectId + "," + block.get(objectId) + "\n");
                }

                blockIds = lockedBlocks.keySet().stream().mapToInt(Integer::intValue).toArray();
                Arrays.sort(blockIds);
                for (int blockId : blockIds) {
                    Map<Integer, Long> block = lockedBlocks.get(blockId);
                    for (int objectId : block.keySet())
                        bw.write("Locked," + blockId + "," + objectId + "," + block.get(objectId) + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
