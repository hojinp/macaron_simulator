package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.commons.OSPriceInfo;
import edu.cmu.pdl.macaronsimulator.simulator.commons.OSPriceInfoGetter;
import edu.cmu.pdl.macaronsimulator.simulator.datalake.Datalake;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.dram.DRAMLRUCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ClusterConfig;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ClusterState;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ClusterUpdatePlan;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigDecision;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.message.ClusterInfoContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.mrc.CostMiniatureSimulation;
import edu.cmu.pdl.macaronsimulator.simulator.mrc.CostTTLMiniatureSimulation;
import edu.cmu.pdl.macaronsimulator.simulator.mrc.LatencyMiniatureSimulation;
import edu.cmu.pdl.macaronsimulator.simulator.profile.CostInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.timeline.TimelineManager;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

/**
 * MacaronMaster is the main component of the MacaronCache. It manages the OSC, OSCMServer, ProxyEngines, and IMCServers.
 * It also manages the reconfiguration of the cluster. It receives the messages from the application, ProxyEngines, and
 * OSCMServer, and generates the events to be registered to the timeline.
 */
public class Controller implements Component {

    public final static String NAME = "CONTROLLER";
    public final static String CACHE_ENGINE_BASENAME = "CACHE_ENGINE";
    public final static String DRAM_CACHE_BASENAME = "DRAM";
    private final static long HR_US = 60L * 60L * 1000L * 1000L;
    private final static long MIN_US = 60L * 1000L * 1000L;

    /* Macaron cache components */
    private OSC osc = null;
    private OSCMServer oscmServer = null;
    private List<CacheEngine> cacheEngines = new ArrayList<>();
    private List<DRAMServer> dramServers = new ArrayList<>();

    /* Variables used for logging information */
    private final ProfileInfoStore profileInfoStore = new ProfileInfoStore();
    private final CostInfoStore costInfoStore = new CostInfoStore();
    private final OSPriceInfo osPriceInfo = OSPriceInfoGetter.getOSPriceInfo();

    /* Variables used for message/event generation */
    private final int newEventMaxCnt = 1000;
    private int newEventIdx = 0;
    private final Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    /* Variables used for DRAM cluster reconfiguration */
    private ClusterState state = ClusterState.STEADY;
    private ReconfigDecision reconfigDecision = ReconfigDecision.NONE;
    private int dramReconfigWaitingAckCount = 0;
    private final long dramReconfigTime = 10L * 60L * 1000L * 1000L; /* terminate DRAM reconfiguration after 10 min */
    private int prefetchCount = 0;
    private long prefetchBegTime = 0L;
    public static int LATENCY_MINI_CACHE_COUNT = -1;
    public static double LATENCY_MINISIM_SAMPLING_RATE = 0.05;
    private String dramReconfigFilename = Paths.get(ProfileInfoStore.LOG_DIRNAME, "dram_reconfig.csv").toString();
    private final static int MIN_REQ_COUNT = 500, SCALE_IN_THRESHOLD = 4;
    private int scaleInCounter = 0;
    private List<Integer> scaleInOptions = new ArrayList<>();
    private long nextDRAMReconfigTime = 0L;

    /* Variables used for OSC reconfiguration/eviction */
    private static long prevAccessHistoryTs = 0L;
    private int oscCacheSizeMB = 0;
    private int oscCacheTTLMin = 0;

    /* Miniature Simulation related variables */
    public static String miniSimDirname = null;
    private CostMiniatureSimulation costMiniSim = null;
    private CostTTLMiniatureSimulation costTTLMiniSim = null;
    private LatencyMiniatureSimulation latencyMiniSim = null;
    private long nextOSCReconfigTime = 0L;
    private String optimalOSCSizeLogFilename = Paths.get(ProfileInfoStore.LOG_DIRNAME, "OSCOptResult.csv").toString();
    private String ttlDecisionLogFilename = Paths.get(ProfileInfoStore.LOG_DIRNAME, "TTLOptResult.csv").toString();

    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public static String preparedSolution = "";
    private Map<Integer, Integer> preparedOSCSolution = new HashMap<>();
    private Map<Integer, Integer> preparedDRAMSolution = new HashMap<>();

    public Controller() {
        MacConf.CONTROLLER_NAME = NAME;

        Logger.getGlobal().info("Controller starts preparing components");

        // Initialize OSC related components
        if (CacheEngine.IS_OSC_ENABLED) {
            this.osc = new OSC(Datalake.maxObjectId);
            this.oscmServer = new OSCMServer(Datalake.maxObjectId);
            this.oscCacheSizeMB = (int) (MacConf.DEFAULT_OSC_CAPACITY / 1024L / 1024L);
            this.oscCacheTTLMin = (int) (MacConf.TTL / 60L / 1000L / 1000L);
            if (ReconfigEvent.IS_OSC_OPT_ENABLED) {
                this.nextOSCReconfigTime = ReconfigEvent.oscReconfigStTime;
                if (ReconfigEvent.oscReconfigInterval == -1L || ReconfigEvent.oscReconfigStTime == -1)
                    throw new RuntimeException("OSC reconfiguration interval or start time is not properly set");
            }
        }

        if ((CacheEngine.IS_DRAM_ENABLED && MacConf.CACHE_NODE_COUNT == 0)
                || (!CacheEngine.IS_DRAM_ENABLED && MacConf.CACHE_NODE_COUNT > 0))
            throw new RuntimeException("Cache node count is not properly set");

        // Initialize DRAM servers
        if (CacheEngine.IS_DRAM_ENABLED) {
            if (MacConf.DRAM_CACHE_TYPE == CacheType.LRU)
                DRAMLRUCache.prepare(Datalake.maxObjectId);
            for (int i = 0; i < MacConf.CACHE_NODE_COUNT; i++) {
                String dramServerName = getDRAMName(i + 1);
                dramServers.add(new DRAMServer(dramServerName));
                ClusterConfig.dramServerNames.add(dramServerName);
            }
            ClusterConfig.nodeIdx = MacConf.CACHE_NODE_COUNT + 1; // Next machine index for the future reconfigurations

            if (ReconfigEvent.IS_DRAM_OPT_ENABLED) {
                if (LATENCY_MINI_CACHE_COUNT <= 0)
                    throw new RuntimeException("LATENCY_MINI_CACHE_COUNT is not properly set");
                this.nextDRAMReconfigTime = ReconfigEvent.dramReconfigStTime;
                if (preparedSolution.length() == 0) {
                    latencyMiniSim = new LatencyMiniatureSimulation(LATENCY_MINISIM_SAMPLING_RATE, LATENCY_MINI_CACHE_COUNT,
                            dramServers.get(0).getCacheGB() * 1024, Datalake.maxObjectId, MacConf.DEFAULT_OSC_CAPACITY);
                }
                writeLineToFile("Minute,PrvIMCSize,NewIMCSize,ExpectedLat\n", dramReconfigFilename, true);
            }

            if (ReconfigEvent.DRAM_OPT_IS_MAIN) {
                this.nextDRAMReconfigTime = ReconfigEvent.dramReconfigStTime;
                writeLineToFile("Minute,PrvDRAMCount,NewDRAMCount\n", dramReconfigFilename, true);
            }
        }

        // Initialize cache engines
        final int cacheEngineCnt = CacheEngine.IS_DRAM_ENABLED ? MacConf.CACHE_NODE_COUNT : 1;
        for (int i = 0; i < cacheEngineCnt; i++) {
            String cacheEngineName = getCacheEngineName(i + 1);
            cacheEngines.add(new CacheEngine(cacheEngineName));
            ClusterConfig.cacheEngineNames.add(cacheEngineName);
            if (CacheEngine.IS_DRAM_ENABLED)
                cacheEngines.get(i).setNodeLocator();
        }
        CacheEngine.prepare(Datalake.maxObjectId);

        // Initialize Miniature Simulation with the given directory that has pre-computed results
        if (preparedSolution.length() == 0) {
            if (miniSimDirname == null)
                costMiniSim = null;
            else if (MacConf.OSC_CACHE_TYPE == CacheType.LRU)
                costMiniSim = new CostMiniatureSimulation(miniSimDirname);
            else if (MacConf.OSC_CACHE_TYPE == CacheType.TTL)
                costTTLMiniSim = new CostTTLMiniatureSimulation(miniSimDirname);
            else
                throw new RuntimeException("Invalid OSC cache type: " + MacConf.OSC_CACHE_TYPE);
        }

        // If there is a prepared solution, then use it
        if (preparedSolution.length() > 0) {
            try {
            // read prepared solution file, each line is minute, osc size, dram size
            BufferedReader reader = new BufferedReader(new FileReader(preparedSolution));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("Minute"))
                    continue;
                preparedDRAMSolution.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                preparedOSCSolution.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[2]));
            }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Prepared solution file is not properly read");
            }
        }

        // Register message handlers
        msgHandlers.put(CompType.APP, this::applicationMsgHandler);
        msgHandlers.put(CompType.ENGINE, this::cacheEngineMsgHandler);
        msgHandlers.put(CompType.OSCM, this::oscmMsgHandler);

        Logger.getGlobal().info("Controller finished preparing components");
    }

    @Override
    public CompType getComponentType() {
        return CompType.CONTROLLER;
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(long ts, Message msg) {
        newEventIdx = 0;
        msgHandlers.get(msg.getFlow() == MsgFlow.RECV ? msg.getSrcType() : msg.getDstType()).accept(ts, msg);
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * Message handler for the application messages
     * 
     * @param ts timestamp when the message is received
     * @param msg message received from the application
     */
    private void applicationMsgHandler(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP || msg.getQType() != QType.ACK)
                    throw new RuntimeException("Message type or Query type is not correct");
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Message type is not correct");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Message handler for the cache engine messages
     * 
     * @param ts timestamp when the message is received
     * @param msg message received from the cache engine
     */
    private void cacheEngineMsgHandler(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Message type is not correct");

                switch (msg.getQType()) {
                    case ACK:
                        // This section handles ACK messages:
                        // 1. For sending consistent hashing info to CacheEngines.
                        // 2. For sending TERMINATE message to the MacServer.
                        //
                        // For (1), if all expected ACK messages are received, we proceed to send the new
                        // consistent hashing information to the application.
                        //
                        // For (2), each ACK message triggers the removal of CacheEngine and DRAM server
                        // from the MacaronMaster.
                        if (dramReconfigWaitingAckCount == 0)
                            throw new RuntimeException("ACK message is not properly handled");

                        if (state == ClusterState.RECONFIG) { // Case (1)
                            if (--dramReconfigWaitingAckCount == 0) {
                                Logger.getGlobal().info("All ACK messages are received. Send new cache engine names to Applicaiton: " + ClusterConfig.cacheEngineNames.toString());
                                Message newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.RECONFIG,
                                        MsgType.REQ, NAME, CompType.CONTROLLER, MacConf.APP_NAME, CompType.APP)
                                        .setCdata(new ClusterInfoContent(ClusterConfig.cacheEngineNames));
                                addEvent(new Pair<>(ts, newMsg));
                            }
                        } else if (state == ClusterState.TERMINATE) { // Case (2)
                            if (--dramReconfigWaitingAckCount == 0) { // all machines are terminated, change to steady state
                                state = ClusterState.STEADY;
                                System.gc();
                                for (String cacheEngine : ClusterConfig.cacheEngineNames) {
                                    Message newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.CLEAR, 
                                        MsgType.REQ, NAME, CompType.CONTROLLER, cacheEngine, CompType.ENGINE)
                                        .setCdata(new ClusterInfoContent(ClusterConfig.cacheEngineNames));
                                    addEvent(new Pair<>(ts, newMsg));
                                }
                            }

                            String engineName = msg.getSrc();
                            String dramName = getDRAMName(getCacheEngineIdx(engineName));
                            int cidx = IntStream.range(0, cacheEngines.size())
                                    .filter(i -> cacheEngines.get(i).NAME.equals(engineName)).findFirst().orElse(-1);
                            int didx = IntStream.range(0, dramServers.size())
                                    .filter(i -> dramServers.get(i).getName().equals(dramName)).findFirst().orElse(-1);
                            if (cidx == -1 || didx == -1)
                                throw new RuntimeException("Corresponding cache engine or dram server is not found");
                            cacheEngines.remove(cidx);
                            dramServers.remove(didx);
                            TimelineManager.delayedDeregisterComponent(ts, engineName);
                            TimelineManager.delayedDeregisterComponent(ts, dramName);
                        } else {
                            throw new RuntimeException("Cannot reach here");
                        }
                        break;

                    case PREFETCH_DONE:
                        // Prefetching occurs when the cluster scales out. There are two scenarios in which the 
                        // CacheEngine sends a PREFETCH_DONE message:
                        // 1. When a new node is launched, and its memory becomes full, it registers a ReconfigEvent 
                        //    to trigger the endReconfig function.
                        // 2. When an existing node cleans up its items after reconfiguration and subsequently 
                        //    prefetches until its memory becomes full again. No specific action is taken.
                        if (!ReconfigEvent.IS_PREFETCH_ENABLED)
                            throw new RuntimeException("PREFETCH_DONE msg cannot be sent when PREFETCH is disabled");
                        if (state == ClusterState.RECONFIG) {
                            if (--prefetchCount == 0) { // Message only comes from the new machines after scale-out
                                Logger.getGlobal().info("All prefetching is done");
                                long elapsedTime = ts - prefetchBegTime;
                                long delay = elapsedTime > dramReconfigTime ? 0L : dramReconfigTime - elapsedTime;
                                addEvent(new Pair<>(ts + delay, new ReconfigEvent(false)));
                            }
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("REQ message is expected.");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Message handler for the OSCM server messages
     * 
     * @param ts timestamp when the message is received
     * @param msg message received from the OSCM server
     */
    public void oscmMsgHandler(final long ts, final Message msg) {
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP || msg.getQType() != QType.RECONFIG)
                    throw new RuntimeException("Message type or Query type is not correct");

                // Send data to be prefetched to the CacheEngines
                if (ReconfigEvent.IS_PREFETCH_ENABLED && state != ClusterState.STEADY) {
                    prefetchCount = 0;
                    for (String cacheEngineName : ClusterConfig.cacheEngineNames) {
                        // Wait for the new machines' prefetching (when SCALE_OUT)
                        if (!ClusterConfig.prevCacheEngineNames.contains(cacheEngineName)) 
                            prefetchCount++;

                        // Send prefetch messages to all machines to register scan order to all the machines 
                        // (i.e., existing machines won't do anything, but save this infromation)
                        Message newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.PREFETCH, MsgType.REQ,
                                NAME, CompType.CONTROLLER, cacheEngineName, CompType.ENGINE).setScanOrder(
                                        msg.getScanOrder(), msg.getScanOrderSize(), msg.getScanOrderNode(),
                                        msg.getScanOrderCount());
                        addEvent(new Pair<>(ts, newMsg));
                    }
                    Logger.getGlobal().info("PREFETCH message is sent to " + ClusterConfig.cacheEngineNames.toString());
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.REQ || msg.getQType() != QType.RECONFIG)
                    throw new RuntimeException("Message type or Query type is not correct");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Initialize the reconfiguration process. This function is invoked only when the RECONFIG event is triggered.
     * 
     * @param ts timestamp when the reconfiguration is triggered
     * @return list of the events to be registered to the timeline
     */
    public Pair<Pair<Long, Event>[], Integer> initReconfig(final long ts) {
        if (!ReconfigEvent.DRAM_OPT_IS_MAIN && !CacheEngine.IS_OSC_ENABLED)
            return new Pair<>(newEvents, 0);

        state = ClusterState.RECONFIG;
        newEventIdx = 0;

        Message oscmMsg = null;
        if (!ReconfigEvent.DRAM_OPT_IS_MAIN) {
            // Load the access history from the previous reconfig time until the current timestamp
            TraceDataDB.loadDataInTimeRange(prevAccessHistoryTs, ts);
            prevAccessHistoryTs = ts;

            // The message that will be sent to the OSCM server for reconfiguration. Need to build this message step by step.
            oscmMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.RECONFIG, MsgType.REQ, NAME, CompType.CONTROLLER, MacConf.OSCM_NAME, CompType.OSCM);
        }

        // Optimize DRAM server cluster configuration. First, Controller generates expected average latency graph that
        // is used for DRAM cluster reconfiguration. Then, it chooses the optimal DRAM cluster capacity that satisfies
        // the latency requirement.
        if (ReconfigEvent.IS_DRAM_OPT_ENABLED || ReconfigEvent.DRAM_OPT_IS_MAIN) {
            assert(!(ReconfigEvent.IS_DRAM_OPT_ENABLED && ReconfigEvent.DRAM_OPT_IS_MAIN));

            // Generate expected latency graph using miniature simulation. Even if the optimization is not triggered 
            // yet, we need to run latency miniature simulation to maintain the cache status.
            Map<Integer, Long> elg = null;
            if (ReconfigEvent.IS_DRAM_OPT_ENABLED && preparedSolution.length() == 0) {
                elg = latencyMiniSim.run(ts);
            }

            if (ts >= nextDRAMReconfigTime) {
                if (ReconfigEvent.DRAM_OPT_MAIN_ONCE)
                    nextDRAMReconfigTime = Long.MAX_VALUE;
                else
                    nextDRAMReconfigTime += ReconfigEvent.dramReconfigInterval;
                ClusterUpdatePlan plan = optimizeDRAM(ts, elg);
                Pair<List<Integer>, Integer> configs = plan.getPlan(); // list of stop nodes, number of new nodes
                List<Integer> stopNodes = configs.getFirst();
                int newNodeCnt = configs.getSecond();
                if (dramReconfigWaitingAckCount > 0)
                    throw new RuntimeException("Previous DRAM reconfiguration is not properly finished yet.");

                // Save the current cluster configuration in the ClusterConfig.prev variables
                ClusterConfig.prevCacheEngineNames = new ArrayList<>(ClusterConfig.cacheEngineNames);
                ClusterConfig.prevDRAMServerNames = new ArrayList<>(ClusterConfig.dramServerNames);

                if (stopNodes.size() == 0 && newNodeCnt == 0) { // nothing changes
                    state = ClusterState.STEADY;
                    reconfigDecision = ReconfigDecision.NONE;
                } else if (stopNodes.size() > 0 && newNodeCnt > 0) { /* Scale-in && out at the same time */
                    throw new RuntimeException("Cannot reach here.");
                } else if (stopNodes.size() > 0) { /* stop machines */
                    reconfigDecision = ReconfigDecision.SCALE_IN;
                    for (int stopNodeIdx : stopNodes) {
                        ClusterConfig.cacheEngineNames.remove(getCacheEngineName(stopNodeIdx));
                        ClusterConfig.dramServerNames.remove(getDRAMName(stopNodeIdx));
                    }

                    // Send previous consistent hashing information to the remaining CacheEngines
                    for (String cacheEngineName : ClusterConfig.cacheEngineNames) {
                        Message msg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.RECONFIG, MsgType.REQ, NAME,
                                CompType.CONTROLLER, cacheEngineName, CompType.ENGINE)
                                .setCdata(new ClusterInfoContent(ClusterConfig.prevCacheEngineNames));
                        addEvent(new Pair<>(ts, msg));
                        dramReconfigWaitingAckCount += 1;
                    }
                } else { /* start new machines */
                    reconfigDecision = ReconfigDecision.SCALE_OUT;
                    for (int i = 0; i < newNodeCnt; i++) {
                        int newNodeIdx = ClusterConfig.nodeIdx + i;

                        // Add new DRAM to the ClusterConfig
                        final String dramServerName = getDRAMName(newNodeIdx);
                        DRAMServer newDRAMServer = new DRAMServer(dramServerName);
                        dramServers.add(newDRAMServer);
                        ClusterConfig.dramServerNames.add(dramServerName);
                        TimelineManager.registerComponent(dramServerName, newDRAMServer);

                        // Add new CacheEngine to the ClusterConfig
                        final String cacheEngineName = getCacheEngineName(newNodeIdx);
                        CacheEngine newCacheEngine = new CacheEngine(cacheEngineName);
                        cacheEngines.add(newCacheEngine);
                        ClusterConfig.cacheEngineNames.add(cacheEngineName);
                        TimelineManager.registerComponent(cacheEngineName, newCacheEngine);
                        newCacheEngine.setNodeLocator();

                        // Send previous consistent hashing information to the new ProxyEngines
                        Message msg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.NEW_RECONFIG, MsgType.REQ,
                                NAME, CompType.CONTROLLER, cacheEngineName, CompType.ENGINE)
                                .setCdata(new ClusterInfoContent(ClusterConfig.prevCacheEngineNames));
                        addEvent(new Pair<>(ts, msg));
                        dramReconfigWaitingAckCount++;
                    }
                    ClusterConfig.nodeIdx += newNodeCnt;
                }

                if (state != ClusterState.STEADY) {
                    if (ReconfigEvent.IS_PREFETCH_ENABLED) { // List up the items in LRU order for DRAM server prefetching
                        prefetchBegTime = ts;
                        long prefetchSize = dramServers.get(0).getCacheCapacity() * dramServers.size();
                        oscmMsg.setDRAMPrefetchSize(prefetchSize * 2);
                        oscmMsg.setCdata(new ClusterInfoContent(ClusterConfig.cacheEngineNames));

                        if (reconfigDecision == ReconfigDecision.SCALE_IN)
                            addEvent(new Pair<>(ts + dramReconfigTime, new ReconfigEvent(false)));
                    } else { // If prefetching is disabled, stop the reconfiguration after dramReconfigTime seconds
                        addEvent(new Pair<>(ts + dramReconfigTime, new ReconfigEvent(false)));
                    }
                }

                // Print out the DRAM reconfiguration result
                if (stopNodes.size() > 0 || newNodeCnt > 0)
                    printDRAMReconfigResult((double) ts / HR_US);
            } else {
                state = ClusterState.STEADY;
            }
        }

        // Optimize OSC size. First, Controller generates expected cost graph that is used for OSC reconfiguration. Then,
        // it chooses the optimal OSC size that minimizes the cost.
        if (ReconfigEvent.IS_OSC_OPT_ENABLED && ts >= nextOSCReconfigTime) {
            int curMin = (int) (ts / MIN_US);
            if (profileInfoStore.hasProfileInfoAtMin(curMin)) {
                if (MacConf.OSC_CACHE_TYPE == CacheType.LRU) {
                    int newOscCacheSizeMB = optimizeOSCSizeMB(curMin);
                    if (newOscCacheSizeMB != -1) {
                        oscCacheSizeMB = newOscCacheSizeMB;
                        oscmMsg.setOSCSizeMB(oscCacheSizeMB);
                        if (ReconfigEvent.IS_DRAM_OPT_ENABLED && preparedSolution.length() == 0)
                            latencyMiniSim.updateOSCSize(oscCacheSizeMB);
                        ReconfigEvent.IS_OSC_OPT_ENABLED = !ReconfigEvent.IS_OSC_OPT_ONCE;
                    }
                } else if (MacConf.OSC_CACHE_TYPE == CacheType.TTL) {
                    int newTTLMin = optimizeOSCTTLMin(curMin);
                    if (newTTLMin != -1) {
                        oscCacheTTLMin = newTTLMin;
                        oscmMsg.setOSCTTLMin(oscCacheTTLMin);
                        if (ReconfigEvent.IS_DRAM_OPT_ENABLED && preparedSolution.length() == 0)
                            throw new RuntimeException("TTL cache type is not supported in the latency miniature simulation");
                        ReconfigEvent.IS_OSC_OPT_ENABLED = !ReconfigEvent.IS_OSC_OPT_ONCE;
                    }
                } else {
                    throw new RuntimeException("Invalid OSC cache type: " + MacConf.OSC_CACHE_TYPE);
                }
                nextOSCReconfigTime += ReconfigEvent.oscReconfigInterval;
            }
        }

        // Send the message that will be sent to the OSCM server for reconfiguration
        if (CacheEngine.IS_OSC_ENABLED)
            addEvent(new Pair<>(ts, oscmMsg));

        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * Use the expected cost curve to find the optimal OSC size.
     * 
     * @param curMin current time (minute)
     * @return optimal OSC size
     */
    private int optimizeOSCSizeMB(int curMin) {
        if (preparedSolution.length() > 0) {
            return preparedOSCSolution.getOrDefault(curMin, oscCacheSizeMB);
        }

        int[] cacheSizeMBs = costMiniSim.getCacheSizeMBs();
        double[] mrc = costMiniSim.getMRC(curMin);
        long[] bmc = costMiniSim.getBMC(curMin);
        if (mrc == null || bmc == null)
            return -1;
        Pair<Integer, Integer> getAndPutcnts = profileInfoStore.getGetAndPutCnts(curMin);
        int getCnt = getAndPutcnts.getFirst(), putCnt = getAndPutcnts.getSecond();
        double avgBlockPortion = profileInfoStore.getBlockEffectivePortion(curMin);

        int optimalCacheSizeMB = -1;
        double minCost = Double.MAX_VALUE;
        for (int i = 0; i < cacheSizeMBs.length; i++) {
            double cost = computeExpectedCost(curMin, cacheSizeMBs[i], mrc[i], bmc[i], getCnt, putCnt, avgBlockPortion, -1);
            optimalCacheSizeMB = cost < minCost ? cacheSizeMBs[i] : optimalCacheSizeMB;
            minCost = cost < minCost ? cost : minCost;
        }
        if (optimalCacheSizeMB == -1)
            throw new RuntimeException("Optimal cache size of OSC is not properly decided.");

        return optimalCacheSizeMB;
    }
    
    /**
     * Use the expected cost curve to find the optimal OSC TTL.
     * 
     * @param curMin current time (minute)
     * @return optimal OSC TTL
     */
    private int optimizeOSCTTLMin(int curMin) {
        assert preparedSolution.length() == 0;

        int[] ttlHRs = costTTLMiniSim.getTTLHRs();
        double[] mrc = costTTLMiniSim.getMRC(curMin);
        long[] bmc = costTTLMiniSim.getBMC(curMin);
        long[] csc = costTTLMiniSim.getCSC(curMin);
        if (mrc == null || bmc == null || csc == null)
            return -1;
        Pair<Integer, Integer> getAndPutcnts = profileInfoStore.getGetAndPutCnts(curMin);
        int getCnt = getAndPutcnts.getFirst(), putCnt = getAndPutcnts.getSecond();
        double avgBlockPortion = profileInfoStore.getBlockEffectivePortion(curMin);

        int optimalTTLMin = -1;
        double minCost = Double.MAX_VALUE;
        for (int i = 0; i < ttlHRs.length; i++) {
            int cacheSizeMB = (int) (csc[i] / 1024L / 1024L);
            assert cacheSizeMB >= 0L;
            double cost = computeExpectedCost(curMin, cacheSizeMB, mrc[i], bmc[i], getCnt, putCnt, avgBlockPortion, ttlHRs[i]);
            optimalTTLMin = cost < minCost ? ttlHRs[i] * 60 : optimalTTLMin;
            minCost = cost < minCost ? cost : minCost;
        }
        if (optimalTTLMin == -1)
            throw new RuntimeException("Optimal cache size of OSC is not properly decided.");

        writeTTLDecisionLog(curMin, optimalTTLMin / 60);

        return optimalTTLMin;
    }

    /**
     * Use the expected cost curve to find the optimal DRAM size.
     * 
     * @param curMin current time (minute)
     * @return optimal DRAM size
     */
    private int optimizeDRAMSizeMBCost(int curMin) {
        int[] cacheSizeMBs = costMiniSim.getCacheSizeMBs();
        double[] mrc = costMiniSim.getMRC(curMin);
        long[] bmc = costMiniSim.getBMC(curMin);
        if (mrc == null || bmc == null)
            return -1;

        int optimalDRAMSizeMB = -1;
        double minCost = Double.MAX_VALUE;
        for (int i = 0; i < cacheSizeMBs.length; i++) {
            double cost = computeExpectedCostDRAM(curMin, cacheSizeMBs[i], bmc[i]);
            optimalDRAMSizeMB = cost < minCost ? cacheSizeMBs[i] : optimalDRAMSizeMB;
            minCost = cost < minCost ? cost : minCost;
        }
        if (optimalDRAMSizeMB == -1)
            throw new RuntimeException("Optimal cache size of OSC is not properly decided.");

        return optimalDRAMSizeMB;
    }

    /**
     * Compute the expected cost of accessing remote data for the given OSC size.
     * 
     * @param curMin current time (minute)
     * @param cacheSizeMB OSC size
     * @param mr miss ratio
     * @param mb miss bytes
     * @param getCnt get count
     * @param putCnt put count
     * @param avgBlockPortion average block portion that is occupied
     * @return expected cost for the next day
     */
    private double computeExpectedCost(int curMin, int cacheSizeMB, double mr, long mb, int getCnt, int putCnt,
            double avgBlockPortion, int ttlHR) {
        double capacityCost = (cacheSizeMB / 1024.)
                * (osPriceInfo.getCapacity() / 60. * ReconfigEvent.getStatWindowMin()) / avgBlockPortion;
        double dataTransferCost = (mb / 1024. / 1024. / 1024.) * osPriceInfo.getTransfer();
        double putOpCost = (getCnt * mr + putCnt) * osPriceInfo.getPut();
        double infraCost = 0.252 * ReconfigEvent.getStatWindowMin(); // VM for Controller (r5.xlarge)
        writeOptimalCacheSizeLog(curMin, cacheSizeMB, capacityCost, dataTransferCost, putOpCost, infraCost, ttlHR);
        return capacityCost + dataTransferCost + putOpCost + infraCost;
    }

    /**
     * Compute the expected cost of accessing remote data for the given DRAM size.
     * 
     * @param curMin current time (minute)
     * @param cacheSizeMB DRAM size
     * @param mr miss ratio
     * @param mb miss bytes
     * @param getCnt get count
     * @param putCnt put count
     * @param avgBlockPortion average block portion that is occupied
     * @return expected cost for the next day
     */
    private double computeExpectedCostDRAM(int curMin, int cacheSizeMB, long mb) {
        int dramServerSizeGB = dramServers.get(0).getCacheGB();
        double dramServerCost = dramServers.get(0).getCost();
        int numberOfNodes = ((int) (cacheSizeMB / 1024. + dramServerSizeGB - 1)) / dramServerSizeGB;
        double capacityCost = dramServerCost * numberOfNodes / 60. * ReconfigEvent.getStatWindowMin();
        double dataTransferCost = (mb / 1024. / 1024. / 1024.) * osPriceInfo.getTransfer();
        double putOpCost = 0.;
        double infraCost = 0.252 * ReconfigEvent.getStatWindowMin(); // VM for Controller (r5.xlarge)
        writeOptimalCacheSizeLog(curMin, cacheSizeMB, capacityCost, dataTransferCost, putOpCost, infraCost, -1);
        return capacityCost + dataTransferCost + putOpCost + infraCost;
    }

    /**
     * Print out the DRAM reconfiguration result.
     * 
     * @param hr current hour
     */
    private void printDRAMReconfigResult(double hr) {
        Logger.getGlobal().info("** Start a valid reconfiguration **");
        Logger.getGlobal().info(String.format("Valid reconfiguration is triggered: %.2f HR", hr));
        Logger.getGlobal().info("Prev cache engines: " + ClusterConfig.prevCacheEngineNames.toString());
        Logger.getGlobal().info("New cache engines: " + ClusterConfig.cacheEngineNames.toString());
        Logger.getGlobal().info("Reconfiguration decision: " + reconfigDecision.toString());
        Logger.getGlobal().info(String.format("Prefetching: %s", ReconfigEvent.IS_PREFETCH_ENABLED));
        Logger.getGlobal().info("***********************************");
    }

    /**
     * Write the expected cost details to the log file.
     * 
     * @param curMin current time (minute)
     * @param cacheSizeMB OSC size
     * @param capacityCost cost for the capacity
     * @param dataTransferCost cost for the data transfer
     * @param putOpCost cost for the put operation
     * @param infraCost cost for the infrastructure (Controller VM)
     */
    private void writeOptimalCacheSizeLog(int curMin, int cacheSizeMB, double capacityCost, double dataTransferCost,
            double putOpCost, double infraCost, int ttlHR) {
        try {
            if (!Files.exists(Paths.get(optimalOSCSizeLogFilename))) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(optimalOSCSizeLogFilename));
                if (ttlHR == -1)
                    writer.write("Time(min),CacheSizeMB,ExpectedCC,ExpectedDTC,ExpectedPutCost,ExpectedInfraCost,ExpectedTotalCost\n");
                else
                    writer.write("Time(min),TTL(hr),ExpectedCC,ExpectedDTC,ExpectedPutCost,ExpectedInfraCost,ExpectedTotalCost\n");
                writer.close();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(optimalOSCSizeLogFilename, true));
            writer.write(String.format("%d,%d,%.4f,%.4f,%.4f,%.4f,%.4f\n", curMin, ttlHR == -1 ? cacheSizeMB : ttlHR, capacityCost,
                            dataTransferCost, putOpCost, infraCost, capacityCost + dataTransferCost + putOpCost + infraCost));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write the TTL decision details to the log file.
     * 
     * @param curMin current time (minute)
     * @param ttlHR optimal TTL in hours
     */
    private void writeTTLDecisionLog(int curMin, int ttlHR) {
        try {
            if (!Files.exists(Paths.get(ttlDecisionLogFilename))) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(ttlDecisionLogFilename));
                writer.write("Time(min),TTL(hr)\n");
                writer.close();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(ttlDecisionLogFilename, true));
            writer.write(String.format("%d,%d\n", curMin, ttlHR));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Wrap up the reconfiguration process.
     * 
     * @param ts timestamp when the reconfiguration should be stopped
     */
    public Pair<Pair<Long, Event>[], Integer> endReconfig(final long ts) {
        Logger.getGlobal().info("Wrapping up the reconfiguration process");

        newEventIdx = 0;
        if (reconfigDecision == ReconfigDecision.NONE) { // endReconfig is called without initReconfig
            throw new RuntimeException("Cannot reach here");
        } else if (reconfigDecision == ReconfigDecision.SCALE_IN) { // DRAM cluster is scaled in
            state = ClusterState.TERMINATE;
            Logger.getGlobal().info("All prevCacheEngineNames: " + ClusterConfig.prevCacheEngineNames.toString());
            Logger.getGlobal().info("All cacheEngineNames: " + ClusterConfig.cacheEngineNames.toString());
            for (String cacheEngine : ClusterConfig.prevCacheEngineNames) {
                boolean isRunning = ClusterConfig.cacheEngineNames.contains(cacheEngine);
                addEvent(new Pair<>(ts,
                        new Message(Message.RESERVED_ID, MsgFlow.SEND, isRunning ? QType.STEADY : QType.TERMINATE,
                                MsgType.REQ, NAME, CompType.CONTROLLER, cacheEngine, CompType.ENGINE)));
                dramReconfigWaitingAckCount += isRunning ? 0 : 1;
            }
        } else { // DRAM cluster is scaled out
            if (reconfigDecision != ReconfigDecision.SCALE_OUT)
                throw new RuntimeException("Unexpected reconfigDecision: " + reconfigDecision.toString());
            state = ClusterState.STEADY;
            for (String cacheEngine : ClusterConfig.cacheEngineNames) {
                boolean isRunning = ClusterConfig.prevCacheEngineNames.contains(cacheEngine);
                addEvent(new Pair<>(ts,
                        new Message(Message.RESERVED_ID, MsgFlow.SEND, isRunning ? QType.CLEAR : QType.STEADY,
                                MsgType.REQ, NAME, CompType.CONTROLLER, cacheEngine, CompType.ENGINE)
                                .setCdata(isRunning ? new ClusterInfoContent(ClusterConfig.cacheEngineNames) : null)));
            }
        }
        reconfigDecision = ReconfigDecision.NONE;
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * Generate a plan that optimize the cache configuration. It will select the minimum number of machines that
     * satisfies the latency requirement.
     * 
     * If there are less than MIN_REQ_COUNT requests, increase the possiblity of scaling in. If the number of scaling in
     * decision is greater than SCALE_IN_THRESHOLD, scale in the cluster. The scaling in decision is the maximum number
     * of machines that were decided in the previous SCALE_IN_THRESHOLD times (conservative option).
     * 
     * @param ts timestamp when the reconfiguration is triggered
     * @param elg expected latency graph that will be used for the optimization
     * @return new plan for the next reconfiguration
     */
    private ClusterUpdatePlan optimizeDRAM(final long ts, final Map<Integer, Long> elg) {
        final int newNodeCnt, curNodeCnt = ClusterConfig.cacheEngineNames.size();

        if (preparedSolution.length() > 0) {
            int curMin = (int) (ts / MIN_US);
            int dramSizeMB = preparedDRAMSolution.getOrDefault(curMin, curNodeCnt * dramServers.get(0).getCacheGB() * 1024);
            newNodeCnt = ((int) (dramSizeMB / 1024. + dramServers.get(0).getCacheGB() - 1)) / dramServers.get(0).getCacheGB();
            String newLine = String.format("%d,%d,%d\n", curMin, curNodeCnt, newNodeCnt);
            writeLineToFile(newLine, dramReconfigFilename, false);
        } else if (ReconfigEvent.DRAM_OPT_IS_MAIN) {
            assert elg == null;
            int curMin = (int) (ts / MIN_US);
            int dramSizeMB = optimizeDRAMSizeMBCost(curMin);
            int dramServerSizeGB = dramServers.get(0).getCacheGB();
            newNodeCnt = Math.max(((int) (dramSizeMB / 1024. + dramServerSizeGB - 1)) / dramServerSizeGB, 1);
            String newLine = String.format("%d,%d,%d\n", curMin, curNodeCnt, newNodeCnt);
            writeLineToFile(newLine, dramReconfigFilename, false);
        } else {
            // Decide the next number of nodes based on the expected latency graph
            if (TraceDataDB.totalAccessCount < MIN_REQ_COUNT) {
                scaleInCounter++;
                newNodeCnt = computeNewNodeCount(curNodeCnt);
            } else {
                if (elg == null) {
                    newNodeCnt = curNodeCnt;
                } else {
                    int cacheSizeMB = optimizeDRAMSizeUsingELG(elg);
                    cacheSizeMB = Math.min(cacheSizeMB, oscCacheSizeMB);
                    final int tmpNewNodeCnt = (int) (cacheSizeMB / 1024L / dramServers.get(0).getCacheGB());

                    if (tmpNewNodeCnt >= curNodeCnt) {
                        scaleInCounter = 0;
                        scaleInOptions.clear();
                        newNodeCnt = tmpNewNodeCnt;
                    } else {
                        scaleInCounter++;
                        scaleInOptions.add(tmpNewNodeCnt);
                        newNodeCnt = computeNewNodeCount(curNodeCnt);
                    }
                }
            }
            
            // Write the current plan to the log file
            if (elg != null) {
                Logger.getGlobal().info("Expected Latency Graph: " + elg.toString());
            }
            Logger.getGlobal().info("NewNodeCount: " + newNodeCnt + ", DRAM Server CacheGB: " + dramServers.get(0).getCacheGB());
            Logger.getGlobal().info("New DRAM Cluster Size: " + String.valueOf(dramServers.get(0).getCacheGB() * 1024 * newNodeCnt));
            String newLine = String.format("%d,%d,%d,%d\n", (int) (ts / MIN_US), curNodeCnt,
                    newNodeCnt, elg == null ? 0 : elg.get(dramServers.get(0).getCacheGB() * 1024 * newNodeCnt));
            writeLineToFile(newLine, dramReconfigFilename, false);
        }

        // Set ClusterUpdatePlan based on the result made by policy
        final ClusterUpdatePlan plan = new ClusterUpdatePlan();
        if (newNodeCnt > curNodeCnt) { // scale out: launch new machines
            plan.newMachineCount(newNodeCnt - curNodeCnt);
        } else if (newNodeCnt < curNodeCnt) { // scale in: stop some machines
            for (int i = 0; i < curNodeCnt - newNodeCnt; i++)
                plan.addStopMachine(getCacheEngineIdx(ClusterConfig.cacheEngineNames.get(i)));
        } else { // do nothing
        }

        return plan;
    }

    /**
     * Compute the number of new machines to be launched. If the number of scaling in decision is greater than 
     * SCALE_IN_THRESHOLD, scale in the cluster. The scaling in decision is the maximum number of machines that were
     * decided in the previous SCALE_IN_THRESHOLD times (conservative option).
     * 
     * @param curNodeCnt current number of machines
     * @return number of new machines to be launched
     */
    private int computeNewNodeCount(int curNodeCnt) {
        if (scaleInCounter >= SCALE_IN_THRESHOLD) {
            scaleInCounter = 0;
            scaleInOptions.clear();
            return Math.max(scaleInOptions.size() == 0 ? 1 : scaleInOptions.stream().max(Integer::compareTo).get(),
                    curNodeCnt == 1 ? 1 : curNodeCnt / 2);
        }
        return curNodeCnt;
    }

    /**
     * Find the knee-point of the given expected latency graph. The knee-point is the point where the curvature is
     * maximum.
     * 
     * @param elg expected latency graph
     * @return knee-point
     */
    private int findKneePoint(Map<Integer, Long> elg) {
        if (elg.size() == 1)
            return elg.entrySet().iterator().next().getKey();

        if (elg.size() < 3)
            throw new RuntimeException("Cannot find knee-point with less than 3 points");

        // Ensure the map is sorted by key
        if (!(elg instanceof TreeMap)) {
            elg = new TreeMap<>(elg);
        }

        // Ensure the map is sorted by key
        TreeMap<Integer, Long> sortedElg = (TreeMap<Integer, Long>) elg;

        // Start and end points
        int x1 = sortedElg.firstKey();
        long y1 = sortedElg.get(x1);
        int x2 = sortedElg.lastKey();
        long y2 = sortedElg.get(x2);

        double maxDistance = Double.MIN_VALUE;
        int kneePoint = x1;
        long kneePointValue = y1;

        for (Map.Entry<Integer, Long> entry : sortedElg.entrySet()) {
            int x = entry.getKey();
            long y = entry.getValue();

            // Calculate perpendicular distance from point to the line
            double numerator = Math.abs((y2 - y1) * x - (x2 - x1) * y + x2 * y1 - y2 * x1);
            double denominator = Math.sqrt(Math.pow(y2 - y1, 2) + Math.pow(x2 - x1, 2));
            double distance = numerator / denominator;

            if (distance > maxDistance) {
                maxDistance = distance;
                kneePoint = x;
                kneePointValue = y;
            }
        }

        for (Map.Entry<Integer, Long> entry : sortedElg.entrySet()) {
            if (entry.getValue() <= kneePointValue) {
                kneePoint = entry.getKey();
                break;
            }
        }

        return kneePoint;
    }

    /**
     * Find the optimal DRAM size using the expected latency graph.
     * 
     * @param elg expected latency graph
     * @return optimal DRAM size
     */
    private int optimizeDRAMSizeUsingELG(Map<Integer, Long> elg) {
        // Ensure the map is sorted by key
        if (!(elg instanceof TreeMap)) {
            elg = new TreeMap<>(elg);
        }

        // Find the smallest key where its value is below the threshold
        for (Map.Entry<Integer, Long> entry : elg.entrySet()) {
            Logger.getGlobal().info("DRAMSize: " + entry.getKey() + ", Latency: " + entry.getValue());
            if (entry.getValue() <= ReconfigEvent.TARGET_LATENCY) {
                return entry.getKey();
            }
        }

        // If not found, calculate knee-point using maximum curvature.
        return findKneePoint(elg);
    }

    /**
     * Add the event to the newEvents array.
     * 
     * @param event event to be added
     */
    private void addEvent(Pair<Long, Event> event) {
        newEvents[newEventIdx++] = event;
    }

    /**
     * Write the line to the given file.
     * 
     * @param line line to be written
     * @param filename file name
     * @param createFile if true, create a new file
     */
    private void writeLineToFile(String line, String filename, boolean createFile) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, !createFile))) {
            writer.write(line);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the name of the cache engine with the given index.
     * 
     * @param index index of the cache engine
     * @return name of the cache engine
     */
    public static String getCacheEngineName(int index) {
        return CACHE_ENGINE_BASENAME + "-" + String.valueOf(index);
    }

    /**
     * Get the index of the cache engine with the given name.
     * 
     * @param name name of the cache engine
     * @return index of the cache engine
     */
    public static int getCacheEngineIdx(String name) {
        if (!name.startsWith(CACHE_ENGINE_BASENAME))
            throw new RuntimeException("CacheEngineName: " + name);
        return Integer.parseInt(name.split("-")[1]);
    }

    /**
     * Get the name of the DRAM server with the given index.
     * 
     * @param index index of the DRAM server
     * @return name of the DRAM server
     */
    public static String getDRAMName(int index) {
        return DRAM_CACHE_BASENAME + "-" + String.valueOf(index);
    }

    /**
     * Get the index of the DRAM server with the given name.
     * 
     * @param name name of the DRAM server
     * @return index of the DRAM server
     */
    public static int getDRAMIdx(String name) {
        if (!name.startsWith(DRAM_CACHE_BASENAME))
            throw new RuntimeException("DRAMName: " + name);
        return Integer.parseInt(name.split("-")[1]);
    }

    /**
     * Get the OSC instance.
     * 
     * @return OSC instance
     */
    public OSC getOSC() {
        return osc;
    }

    /**
     * Get the OSCM server instance.
     * 
     * @return OSCM server instance
     */
    public OSCMServer getOSCMServer() {
        return oscmServer;
    }

    /**
     * Get the list of cache engines.
     * 
     * @return list of cache engines
     */
    public List<CacheEngine> getCacheEngines() {
        return cacheEngines;
    }

    /**
     * Get the list of DRAM servers.
     * 
     * @return list of DRAM servers
     */
    public List<DRAMServer> getDRAMServers() {
        return dramServers;
    }

    @Override
    public ProfileInfo getAndResetProfileInfo() {
        return null;
    }

    @Override
    public void saveProfileInfo(long timestamp, CompType compType, String name, ProfileInfo profileInfo) {
        profileInfoStore.addProfileInfo(timestamp, compType, name, profileInfo);
    }

    /**
     * Save the profile information to the file.
     * 
     * @param timestamp timestamp when the profile information is saved
     */
    public void saveProfileInfosToFile(long timestamp) {
        profileInfoStore.saveProfileInfosToFile(timestamp);
    }

    /**
     * Calculate the cost of to do logging
     * .
     * @param timestamp timestamp when the cost is calculated
     * @param componentType component type
     * @param name component name
     * @param profileInfo profile information
     */
    public void calculateAddCost(long timestamp, CompType compType, String name, ProfileInfo profileInfo) {
        costInfoStore.calculateAddCost(timestamp, compType, name, profileInfo, MacConf.CACHE_NODE_MTYPE);
    }

    /**
     * Save the cost information to the file.
     * 
     * @param timestamp timestamp when the cost information is saved
     */
    public void saveCostInfosToFile(final long timestamp) {
        costInfoStore.saveCostInfosToFile(timestamp);
    }
}
