package edu.cmu.pdl.macaronsimulator.simulator.macaroncache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.commons.MachineInfo;
import edu.cmu.pdl.macaronsimulator.simulator.commons.MachineInfoGetter;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.dram.DRAMCache;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.dram.DRAMLRUCache;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileCounter;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;

public class DRAMServer implements Component {

    private final String NAME;
    private final long cacheCapacityGB;
    private final double cost;
    private final DRAMCache cache;

    private final LatencyGenerator lg;
    private final ProfileCounter profileCounter;

    private final int newEventMaxCnt = 1000;
    private int newEventIdx = 0;
    private Pair<Long, Event> newEvents[] = new Pair[newEventMaxCnt];

    private Map<CompType, BiConsumer<Long, Message>> msgHandlers = new HashMap<>();

    public DRAMServer(final String name) {
        NAME = name;
        MacConf.DRAM_NAME_LIST.add(name);
        MachineInfo machineInfo = MachineInfoGetter.getMachineInfo(MacConf.CACHE_NODE_MTYPE);
        this.cacheCapacityGB = machineInfo.getCapacity();
        this.cost = machineInfo.getOndemandPrice();
        long gbToByte = 1024L * 1024L * 1024L;
        long dramCapacity = MacConf.MANUAL_DRAM_CAPACITY > 0L ? MacConf.MANUAL_DRAM_CAPACITY : cacheCapacityGB * gbToByte;
        if (MacConf.DRAM_CACHE_TYPE == CacheType.LRU) {
            cache = new DRAMLRUCache(Controller.getDRAMIdx(name), dramCapacity);
        } else {
            throw new RuntimeException(MacConf.DRAM_CACHE_TYPE.toString() + " is not implemented");
        }
        this.lg = new LatencyGenerator();
        this.profileCounter = new ProfileCounter();
        this.profileCounter.setCapacity(dramCapacity);
        this.profileCounter.setMachineType(machineInfo.getName());

        // Register message handlers
        msgHandlers.put(CompType.ENGINE, this::cacheEngineMsgHandler);
    }

    @Override
    public CompType getComponentType() {
        return CompType.DRAM;
    }

    /**
     * Get the name of this component.
     * 
     * @return name of this component
     */
    public String getName() {
        return NAME;
    }

    /**
     * Get the cache capacity in bytes.
     * 
     * @return cache capacity in bytes
     */
    public long getCacheCapacity() {
        assert MacConf.MANUAL_DRAM_CAPACITY == -1L;
        long gbToByte = 1024L * 1024L * 1024L;
        return cacheCapacityGB * gbToByte;
    }

    /**
     * Get the cost of this DRAM server.
     * 
     * @return cost of this DRAM server
     */
    public double getCost() {
        return cost;
    }

    /**
     * Get the cache capacity in GB.
     * 
     * @return cache capacity in GB
     */
    public int getCacheGB() {
        assert MacConf.MANUAL_DRAM_CAPACITY == -1L;
        return (int) cacheCapacityGB;
    }

    /**
     * Clear the memory resources (newEvents).
     */
    public void cleanUpResources() {
        newEvents = null;
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
        switch (msg.getFlow()) {
            case RECV:
                CompType srcType = msg.getSrcType();
                long objSize = 0L;
                switch (msg.getQType()) {
                    case GET:
                        profileCounter.queryCounter(QType.GET);
                        objSize = cache.get(msg.getObjId());
                        profileCounter.cacheCounter(objSize > 0L ? true : false);
                        if (objSize > 0L)
                            msg.setDataExists().setObjSize(objSize).setDataSrc(CompType.DRAM);
                        addEvent(new Pair<Long, Event>(
                                ts + lg.get(CompType.DRAM, srcType, objSize < 0L ? 0L : objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case PREFETCH_PUT:
                    case PUT:
                        profileCounter.queryCounter(QType.PUT);
                        objSize = msg.getObjSize();
                        cache.put(msg.getObjId(), objSize);
                        profileCounter.transfer(MsgFlow.RECV, objSize);
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.DRAM, srcType, objSize),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case DELETE:
                        profileCounter.queryCounter(QType.DELETE);
                        objSize = cache.delete(msg.getObjId());
                        addEvent(new Pair<Long, Event>(ts + lg.get(CompType.DRAM, srcType, 0L),
                                msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case TERMINATE: // Clear all the cache data before it is termianted.
                        cache.clear();
                        addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case CLEAR: // Delete all the data that are not belong to this node anymore
                        final List<String> cacheEngineNames = msg.getCdata().getCacheEngineNameList();
                        cache.clear(cacheEngineNames);
                        addEvent(new Pair<>(ts, msg.setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    case PREFETCH: // Return the current cache occupancy
                        addEvent(new Pair<>(ts, msg.setDRAMOccupancy(cache.getOccupiedSize(), cache.getCacheSize())
                                .setFlow(MsgFlow.SEND).setMsgType(MsgType.RESP).swapSrcDst()));
                        break;

                    default:
                        throw new RuntimeException("Cannot reach here, msg.QType: " + msg.getQType().toString());
                }
                break;

            case SEND:
                addEvent(new Pair<Long, Event>(ts, msg.setFlow(MsgFlow.RECV)));
                if (msg.getQType() == QType.GET && msg.getDataExists())
                    profileCounter.transfer(MsgFlow.SEND, msg.getObjSize());
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
        profileCounter.setOccupiedSize(cache.getOccupiedSize());
        return profileCounter.getAndResetProfileInfo();
    }

    @Override
    public void saveProfileInfo(final long timestamp, final CompType compType, final String name,
            final ProfileInfo profileInfo) {
        throw new RuntimeException("This method must never be called.");
    }
}
