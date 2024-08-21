package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;

public class OSC implements Component {
    private static final String NAME = "OBJECT_STORAGE_CACHE";
    private final boolean packing;
    private final Long dataStorage[];
    private long occupiedSize = 0L;
    private final LatencyGenerator latGen;
    private final ProfileCounter profileCounter;

    private final int newEventMaxCnt = 1000;
    private int newEventIdx = 0;
    private final Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public OSC(final int maxObjectId) {
        MacConf.OSC_NAME = NAME;
        packing = MacConf.PACKING;
        latGen = new LatencyGenerator();
        profileCounter = new ProfileCounter();

        if (maxObjectId >= CacheEngine.MAX_BLOCK_ID)
            throw new RuntimeException("Max object id must be smaller than " + CacheEngine.MAX_BLOCK_ID);
        int dataStorageLength = packing ? CacheEngine.MAX_BLOCK_ID : maxObjectId + 1;

        dataStorage = new Long[dataStorageLength];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < dataStorageLength; i += threadLength)
                dataStorage[i] = -1L;
        });

        // Register message handlers
        msgHandlers.put(CompType.ENGINE, this::cacheEngineMsgHandler);
        msgHandlers.put(CompType.OSCM, this::oscmServerMsgHandler);
    }

    @Override
    public CompType getComponentType() {
        return CompType.OSC;
    }

    /**
     * Put for non-packing and packing options
     * 
     * @param id object id of the object
     * @param value size of the object
     */
    public void put(final int id, final long value) {
        if (MacConf.PACKING && dataStorage[id] != -1L)
            throw new RuntimeException("Data must not exist. Key: " + id + ", Value: " + value);
        occupiedSize += (dataStorage[id] != -1L ? value - dataStorage[id] : value);
        dataStorage[id] = value;
    }

    /**
     * Get for packing options
     * 
     * @param id object id to be retrieved
     * @return size of the object
     */
    public long get(final int id) {
        if (dataStorage[id] == -1L)
            throw new RuntimeException("Data must exist. Key: " + id);
        return dataStorage[id];
    }

    /**
     * Get for non-packing and packing options
     * 
     * @param id object id to be retrieved
     * @param value size of the object
     * @return size of the object
     */
    public long get(final int id, final long value) {
        if (value <= 0L)
            throw new RuntimeException("Value must be positive. Key: " + id + ", Value: " + value);
        if (dataStorage[id] == -1L)
            return 0L;
        return value;
    }

    /**
     * delete for non-packing and packing options
     * 
     * @param key key of the object to be deleted
     * @return size of the object to be deleted
     */
    public long delete(final int id) {
        long objSize = dataStorage[id];
        if (objSize == -1L)
            return 0L;
        occupiedSize -= objSize;
        dataStorage[id] = -1L;
        return objSize;
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(long ts, Message msg) {
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
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Message type must be REQ. " + msg.getMsgType());

                long objSize;
                final CompType srcType = msg.getSrcType();
                switch (msg.getQType()) {
                    case GET:
                    case PREFETCH_GET:
                        profileCounter.queryCounter(QType.GET);
                        this.get(packing ? msg.getBlockId() : msg.getObjId(), msg.getObjSize());
                        addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, srcType, msg.getObjSize()),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case PUT:
                        profileCounter.queryCounter(QType.PUT);
                        objSize = packing ? msg.getBlockSize() : msg.getObjSize();
                        this.put(packing ? msg.getBlockId() : msg.getObjId(), objSize);
                        addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, srcType, objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case DELETE:
                        profileCounter.queryCounter(QType.DELETE);
                        if (!packing) {
                            objSize = this.delete(msg.getObjId());
                            addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, srcType, 0L),
                                    msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        } else {
                            throw new RuntimeException("No delete operation is implemented for packing options.");
                        }
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Message type must be RESP. " + msg.getMsgType());
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * MsgHandler that handles messages coming from/going to the OSCMServer.
     * 
     * @param ts timestamp this message is triggered
     * @param msg message that this msgHandler should handle
     */
    private void oscmServerMsgHandler(final long ts, final Message msg) {
        long objSize;
        switch (msg.getFlow()) {
            case RECV:
                assert msg.getMsgType() == MsgType.REQ;
                switch (msg.getQType()) {
                    case GET:
                        profileCounter.queryCounter(QType.GC_GET);
                        objSize = this.get(msg.getBlockId());
                        addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, msg.getSrcType(), objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case PUT:
                        profileCounter.queryCounter(QType.GC_PUT);
                        objSize = msg.getBlockSize();
                        this.put(msg.getBlockId(), objSize);
                        addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, msg.getSrcType(), objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case DELETE:
                        profileCounter.queryCounter(QType.GC_DELETE);
                        objSize = this.delete(packing ? msg.getBlockId() : msg.getObjId());
                        addEvent(new Pair<Long, Event>(ts + latGen.get(CompType.OSC, msg.getSrcType(), 0L),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Message type must be RESP. " + msg.getMsgType());
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Add an event to the newEvents array.
     * 
     * @param event event to be added
     */
    private void addEvent(Pair<Long, Event> event) {
        newEvents[newEventIdx++] = event;
    }

    @Override
    public ProfileInfo getAndResetProfileInfo() {
        profileCounter.setOccupiedSize(occupiedSize);
        profileCounter.setCapacity(occupiedSize);
        return profileCounter.getAndResetProfileInfo();
    }

    @Override
    public void saveProfileInfo(final long timestamp, final CompType componentType, final String name,
            final ProfileInfo profileInfo) {
        throw new RuntimeException("OSC:saveProfileInfo:This method must never be called.");
    }
}
