package edu.cmu.pdl.macaronsimulator.simulator.datalake;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.MacConf;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

/**
 * Datalake component that stores the data.
 */
public class Datalake implements Component {

    private static final String NAME = "DATALAKE";
    private static final long NOT_EXISTS = -1L;

    private Long dataStorage[] = null;
    private static long occupiedSize = 0L;

    public static long totalRequestCount = 0L;
    public static int maxObjectId = 0;

    private final LatencyGenerator lg;
    private final ProfileCounter profileCounter;

    private final int newEventMaxCnt = 1000;
    private int newEventIdx = 0;
    private final Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    // Define msgHandlers that map each CompType to the corresponding msgHandler.
    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public Datalake() {
        MacConf.DATALAKE_NAME = NAME;
        this.lg = new LatencyGenerator();
        this.profileCounter = new ProfileCounter();

        // Register message handlers
        msgHandlers.put(CompType.ENGINE, this::cacheEngineAndApplicationMsgHandler);
        msgHandlers.put(CompType.APP, this::cacheEngineAndApplicationMsgHandler);
    }

    @Override
    public CompType getComponentType() {
        return CompType.DATALAKE;
    }

    /**
     * Put the data in this datalake.
     * 
     * @param key key of the new data
     * @param value size of the item stored in the datalake
     */
    public void put(int decodedOid, long value) {
        if (value <= 0L)
            throw new RuntimeException("Invalid value: " + value);
        occupiedSize += (dataStorage[decodedOid] != NOT_EXISTS ? value - dataStorage[decodedOid] : value);
        dataStorage[decodedOid] = value;
    }

    /**
     * Get the data stored from this datalake.
     * 
     * @param key key of the retrieved item
     * @return value size of the item (if the item does not exist, return NOT_EXISTS)
     */
    public long get(int decodedOid) {
        return dataStorage[decodedOid];
    }

    /**
     * Delete the item corresponding to the key
     * 
     * @param key key of the item to be deleted
     * @return size of the deleted item
     */
    public void delete(int decodedOid) {
        // do nothing
    }

    @Override
    public Pair<Pair<Long, Event>[], Integer> msgHandler(long ts, Message msg) {
        newEventIdx = 0;
        final CompType tgtComp = msg.getFlow() == MsgFlow.RECV ? msg.getSrcType() : msg.getDstType();
        msgHandlers.get(tgtComp).accept(ts, msg);
        return new Pair<>(newEvents, newEventIdx);
    }

    /**
     * MsgHandler that handles messages coming from/going to the ProxyEngine and Application.
     * 
     * @param newEvents after this msgHandler handles the message, new events should be registered to the TimelineManger
     * @param ts timestamp this message is triggered
     * @param msg message that this msgHandler should handle
     */
    private void cacheEngineAndApplicationMsgHandler(long ts, Message msg) {
        long objSize;
        switch (msg.getFlow()) {
            case RECV:
                if (msg.getMsgType() != MsgType.REQ)
                    throw new RuntimeException("Unexpected message type: " + msg.getMsgType());

                final CompType srcType = msg.getSrcType();
                switch (msg.getQType()) {
                    case GET:
                        profileCounter.queryCounter(QType.GET);
                        objSize = this.get(msg.getObjId());
                        if (objSize <= 0L)
                            throw new RuntimeException("Object size must be greater than 0: " + objSize);
                        msg.setDataExists().setObjSize(objSize).setDataSrc(CompType.DATALAKE);
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.DATALAKE, srcType, objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;
                    case PUT:
                        profileCounter.queryCounter(QType.PUT);
                        objSize = msg.getObjSize();
                        this.put(msg.getObjId(), objSize);
                        profileCounter.transfer(MsgFlow.RECV, objSize);
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.DATALAKE, srcType, objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;
                    case DELETE:
                        profileCounter.queryCounter(QType.DELETE);
                        this.delete(msg.getObjId());
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.DATALAKE, srcType, 0L),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;
                    default:
                        throw new RuntimeException("Cannot reach here");
                }
                break;

            case SEND:
                if (msg.getMsgType() != MsgType.RESP)
                    throw new RuntimeException("Unexpected message type: " + msg.getMsgType());
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                if (msg.getQType() == QType.GET && msg.getDataExists())
                    profileCounter.transfer(MsgFlow.SEND, msg.getObjSize());
                break;

            default:
                throw new RuntimeException("Cannot reach here");
        }
    }

    /**
     * Add a new event to the newEvents array.
     * 
     * @param event new event to be added
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
    public void saveProfileInfo(long timestamp, CompType componentType, String name, ProfileInfo profileInfo) {
        throw new RuntimeException("This method must never be called.");
    }

    /**
     * Prepare datalake data for this trace experiment. If the prepared file exists, load the state from the file. 
     * If the prepared file does not exist, prepare the data and save the state to the file.
     * 
     * @param traceFilePath trace file name
     * @return number of requests in this trace
     */
    public void prepareDataForTrace(final String traceFilePath) {
        Logger.getGlobal().info("Start preparing data lake data for the trace: " + traceFilePath);

        Path prepDirName = Paths.get(Paths.get(traceFilePath).getParent().toAbsolutePath().toString(), "prepared");
        Path basePath = Paths.get(traceFilePath).getFileName();
        Path prepFilePath = Paths.get(prepDirName.toAbsolutePath().toString(), basePath.toString());
        try {
            Files.createDirectories(prepDirName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (Files.exists(prepFilePath)) {
            loadDatalakeStateFromFile(prepFilePath);
        } else {
            buildDatalakeState();
            saveDatalakeStateToFile(prepFilePath);
        }

        Logger.getGlobal().info("** Total number of requests: " + totalRequestCount);
        Logger.getGlobal().info("** Maximum object id: " + maxObjectId);
        Logger.getGlobal().info("** Datalake capacity: " + occupiedSize);
        Logger.getGlobal().info("** Done preparing data lake.");
    }

    /**
     * Build the datalake state from the trace file.
     */
    private void buildDatalakeState() {
        maxObjectId = TraceDataDB.getMaxObjectId();
        dataStorage = new Long[maxObjectId + 1];
        Thread threads[] = new Thread[Runtime.getRuntime().availableProcessors()];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = threadId; j < dataStorage.length; j += threads.length)
                    dataStorage[j] = NOT_EXISTS;
            });
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        TraceDataDB.prepareNextRequest();
        while (TraceDataDB.getNextRequest()) {
            // For Get operation, if the object does not exist yet, set the object size
            if (TraceDataDB.seqOpType == 1 && dataStorage[TraceDataDB.seqObjectId] == -1L)
                this.put(TraceDataDB.seqObjectId, TraceDataDB.seqObjectSize);
            totalRequestCount += 1L;
        }
    }

    /**
     * Save the datalake state to the file.
     * @param prepFilePath path to the prepared file
     */
    private void saveDatalakeStateToFile(Path prepFilePath) {
        Logger.getGlobal().info("** Start saving datalake state to the file: " + prepFilePath.toString());

        try (FileOutputStream out = new FileOutputStream(prepFilePath.toString());
                ObjectOutputStream s = new ObjectOutputStream(out)) {
            s.writeLong(totalRequestCount);
            s.writeInt(maxObjectId);
            s.writeLong(occupiedSize);
            s.writeObject(dataStorage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Load the datalake state from the file.
     * @param prepFilePath path to the prepared file
     */
    private void loadDatalakeStateFromFile(Path prepFilePath) {
        Logger.getGlobal().info("** Start loading datalake state from the file: " + prepFilePath.toString());

        Path tmpDirPath = Paths.get("/tmp/macaron/datalake");
        try {
            Files.createDirectories(tmpDirPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path tmpFilePath = Paths.get(tmpDirPath.toString(), prepFilePath.getFileName().toString());
        try {
            if (!Files.exists(tmpFilePath))
                Files.copy(prepFilePath, tmpFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (FileInputStream in = new FileInputStream(tmpFilePath.toString());
                BufferedInputStream bin = new BufferedInputStream(in);
                ObjectInputStream s = new ObjectInputStream(bin)) {
            totalRequestCount = s.readLong();
            maxObjectId = s.readInt();
            occupiedSize = s.readLong();
            dataStorage = (Long[]) s.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the average object size stored in this datalake.
     * 
     * @return average object size in KB
     */
    public static long getAvgObjSizeKB() {
        return occupiedSize / 1024L / maxObjectId;
    }
}
