package edu.cmu.pdl.macaronsimulator.simulator.application;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.MacConf;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.FixedNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.KetamaNodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.nodelocator.NodeLocator;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.ObjContent;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.message.TraceQueryTypeMap;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

/**
 * Application component that generates the requests from the trace file, and sends the requests to the ProxyEngine or
 * the Datalake. Also, it receives the responses from the ProxyEngine or the Datalake, and sends the responses to the
 * ProxyEngine or the Datalake.
 */
public class Application implements Component {

    private static final String NAME = "APPLICATION";

    private boolean isMacaronEnabled;

    private List<String> targets = null;
    private NodeLocator nodeRoute = null;

    private Pair<Long, Message> requestQueue[];
    private int curPtr = 0, lastPtr = 0, maxQueueLength = 1000;
    private AtomicInteger waitingRequestCount = new AtomicInteger(0);
    private long requestId = 0;
    private ExecutorService requestQueueExecutor = Executors.newSingleThreadExecutor();
    private Future<Boolean> requestQueueResult = null;

    private ProfileCounter profileCounter;
    private ApplicationLogWriter logWriter; /* Log all going out/coming in request information */

    private LatencyGenerator lg; /* Latency generator going out from the Application */

    private Map<Long, Long> requestIdToSentTs;
    private long sentMsgCnt = 0L, recvMsgCnt = 0L;

    private int newEventMaxCnt = 1000;
    private int newEventIdx = 0;
    private Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    // Define msgHandlers that map each CompType to the corresponding msgHandler.
    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public Application() {
        MacConf.APP_NAME = NAME;
        this.requestQueue = new Pair[maxQueueLength];
        this.profileCounter = new ProfileCounter();
        this.logWriter = new ApplicationLogWriter();
        this.lg = new LatencyGenerator();
        this.requestIdToSentTs = new HashMap<>();

        // Register message handlers
        msgHandlers.put(CompType.ENGINE, this::cacheEngineMsgHandler);
        msgHandlers.put(CompType.DATALAKE, this::datalakeMsgHandler);
        msgHandlers.put(CompType.CONTROLLER, this::controllerMsgHandler);
    }

    @Override
    public CompType getComponentType() {
        return CompType.APP;
    }

    /**
     * Connect to the other components. This function should be called before {@code #startRequestQueue()}.
     *
     * @param isMacaronEnabled whether the Macaron is enabled or not
     */
    public void connect(final boolean isMacaronEnabled) {
        this.isMacaronEnabled = isMacaronEnabled;
        this.targets = isMacaronEnabled ? MacConf.ENGINE_NAME_LIST
                : new ArrayList<String>(List.of(MacConf.DATALAKE_NAME));
        this.nodeRoute = isMacaronEnabled
                ? (targets.size() == 1 ? new FixedNodeLocator(targets.get(0)) : new KetamaNodeLocator(targets))
                : new FixedNodeLocator(MacConf.DATALAKE_NAME);
    }

    /**
     * Save the latency information to the file.
     * 
     * @param ts timestamp application write the latencies to the file
     */
    public void saveLatencyInfoToFile(long ts) {
        logWriter.saveLatencyInfoToFile(ts);
    }

    /**
    
     * Start the request queue thread.
     *
     * @return the result of the request queue thread
     */
    public Future<Boolean> startRequestQueue() {
        if (targets == null)
            throw new RuntimeException("You must call connect() before startRequestQueue()");

        /* Start the request queue thread */
        TraceDataDB.prepareNextRequest();
        requestQueueResult = requestQueueExecutor.submit(() -> {
            while (TraceDataDB.getNextRequest()) {
                ObjContent obj = TraceDataDB.seqOpType == 0
                        ? new ObjContent(TraceDataDB.seqObjectId, TraceDataDB.seqObjectSize)
                        : new ObjContent(TraceDataDB.seqObjectId);
                String dst = isMacaronEnabled ? null : MacConf.DATALAKE_NAME; // decided when the request is sent
                CompType dstType = isMacaronEnabled ? CompType.ENGINE : CompType.DATALAKE;
                Message request = new Message(requestId++, MsgFlow.SEND, TraceQueryTypeMap.get(TraceDataDB.seqOpType),
                        MsgType.REQ, NAME, CompType.APP, dst, dstType).setObj(obj);
                // if (TraceDataDB.seqOpType == 2)
                //     continue; // ROLLBACK

                requestQueue[lastPtr] = new Pair<>(TraceDataDB.seqTimestamp, request);
                lastPtr = (lastPtr + 1) % maxQueueLength;
                waitingRequestCount.incrementAndGet();
                while (waitingRequestCount.get() == maxQueueLength)
                    ;
            }
            return true;
        });
        return requestQueueResult;
    }

    /**
     * Poll the next request from the pendingRequests. If the request is not yet prepared, this function waits until 
     * the request is prepared.
     * 
     * @return the next request
     */
    public Pair<Long, Message> pollRequest() {
        while (!TraceDataDB.seqDone || waitingRequestCount.get() > 0) {
            // If RequestQueueThread is terminated by error or exception, stop this while loop.
            try {
                if (requestQueueResult.isDone() && !requestQueueResult.get())
                    break;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Next request is not yet prepared
            if (waitingRequestCount.get() == 0)
                continue;

            // Next request is prepared. Return the next request
            final Pair<Long, Message> request = requestQueue[curPtr];
            requestQueue[curPtr] = null;
            curPtr = (curPtr + 1) % maxQueueLength;
            waitingRequestCount.decrementAndGet();
            if (request == null)
                throw new RuntimeException("Request must not be null");

            return request;
        }

        return null;
    }

    /**
     * Stop the request queue thread.
     */
    public void stop() {
        try {
            // logWriter.stop();
            // logWriter.joinThread();
            requestQueueExecutor.shutdown();
            requestQueueExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            if (sentMsgCnt != recvMsgCnt)
                throw new RuntimeException("Sent message count and received message count are different");
            else
                Logger.getGlobal().info("Sent message count and received message count are the same: " + sentMsgCnt);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(final long ts, final Message msg) {
        newEventIdx = 0;
        msgHandlers.get(msg.getFlow() == MsgFlow.RECV ? msg.getSrcType() : msg.getDstType()).accept(ts, msg);
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * MsgHandler that handles messages coming from/going to the CacheEngine.
     *
     * @param ts timestamp this message is triggered
     * @param msg message that this msgHandler should handle
     */
    private void cacheEngineMsgHandler(final long ts, final Message msg) {
        sentMsgCnt += msg.getFlow() == MsgFlow.SEND ? 1L : 0L;
        recvMsgCnt += msg.getFlow() == MsgFlow.RECV ? 1L : 0L;
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Response is expected");
                if (msg.getQType() == QType.GET && msg.getDataExists())
                    logWriter.log(Triple.of(ts, msg.getDataSrc(), ts - requestIdToSentTs.get(msg.getReqId())));
                requestIdToSentTs.remove(msg.getReqId());
                break;
            case SEND:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Request is expected");
                requestIdToSentTs.put(msg.getReqId(), ts);
                long sendSize = msg.getQType() == QType.PUT ? msg.getObjSize() : 0L;
                if (msg.getDstType() == CompType.ENGINE)
                    msg.setDst(nodeRoute.getNode(msg.getObj()), CompType.ENGINE); // after reconfiguration, dst can be changed
                addEvent(new Pair<Long, Event>(ts + lg.get(CompType.APP, CompType.ENGINE, sendSize),
                        msg.setFlow(MsgFlow.RECV)));
                profileCounter.queryCounter(msg.getQType());
                break;
            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * MsgHandler that handles messages coming from/going to the Datalake.
     *
     * @param ts timestamp this message is triggered
     * @param msg message that this msgHandler should handle
     */
    private void datalakeMsgHandler(final long ts, final Message msg) {
        sentMsgCnt += msg.getFlow() == MsgFlow.SEND ? 1L : 0L;
        recvMsgCnt += msg.getFlow() == MsgFlow.RECV ? 1L : 0L;
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Response is expected");
                if (msg.getQType() == QType.GET && msg.getDataExists())
                    logWriter.log(Triple.of(ts, msg.getDataSrc(), ts - requestIdToSentTs.get(msg.getReqId())));
                requestIdToSentTs.remove(msg.getReqId());
                break;
            case SEND:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Request is expected");
                requestIdToSentTs.put(msg.getReqId(), ts);
                long sendSize = msg.getQType() == QType.PUT ? msg.getObjSize() : 0L;
                addEvent(new Pair<Long, Event>(ts + lg.get(CompType.APP, CompType.DATALAKE, sendSize),
                        msg.setFlow(MsgFlow.RECV)));
                profileCounter.queryCounter(msg.getQType());
                break;
            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * MsgHandler that handles messages coming from/going to the MacaronMaster.
     *
     * @param ts timestamp this message is triggered
     * @param msg message that this msgHandler should handle
     */
    private void controllerMsgHandler(final long ts, final Message msg) {
        Message newMsg = null;
        switch (msg.getFlow()) {
            case RECV:
                /* New consistent hashing is received from the MacaronMaster. Send the ACK after updating nodeRoute. */
                if (msg.getQType() != QType.RECONFIG || msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Unexpected message type");
                final List<String> newCacheEngineNames = msg.getCdata().getCacheEngineNameList();
                Logger.getGlobal().info("Application received new cache engines: " + newCacheEngineNames.toString());
                targets = new ArrayList<>(newCacheEngineNames);
                nodeRoute = new KetamaNodeLocator(targets);
                newMsg = new Message(Message.RESERVED_ID, MsgFlow.SEND, QType.ACK, MsgType.RESP, NAME, CompType.APP,
                        MacConf.CONTROLLER_NAME, CompType.CONTROLLER);
                addEvent(new Pair<>(ts, newMsg));
                break;
            case SEND:
                if (msg.getQType() != QType.ACK || msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Unexpected message type");
                addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.RECV)));
                break;
            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Add the event to the newEvents.
     * 
     * @param event event to be added
     */
    private void addEvent(Pair<Long, Event> event) {
        newEvents[newEventIdx++] = event;
    }

    @Override
    public ProfileInfo getAndResetProfileInfo() {
        return profileCounter.getAndResetProfileInfo();
    }

    @Override
    public void saveProfileInfo(final long timestamp, final CompType componentType, final String name,
            final ProfileInfo profileInfo) {
        throw new RuntimeException("This method must never be called.");
    }
}
