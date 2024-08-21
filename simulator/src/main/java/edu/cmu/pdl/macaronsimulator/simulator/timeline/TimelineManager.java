package edu.cmu.pdl.macaronsimulator.simulator.timeline;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.common.ProgressBar;
import edu.cmu.pdl.macaronsimulator.simulator.application.Application;
import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.Component;
import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.CacheEngine;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.Controller;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.DRAMServer;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.MacConf;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgFlow;
import edu.cmu.pdl.macaronsimulator.simulator.message.MsgType;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.profile.CostInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileEvent;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfoStore;

public class TimelineManager {

    private static Timeline timeline;
    private static final Map<String, Component> componentMap = new HashMap<>();

    public TimelineManager(final Timeline timeline) {
        TimelineManager.timeline = timeline;
    }

    /**
     * Register a component to the componentMap.
     * 
     * @param componentString string name of the component
     * @param component component to be registered
     */
    public static void registerComponent(final String componentString, final Component component) {
        if (componentMap.containsKey(componentString))
            Logger.getGlobal().warning("Component " + componentString + " already exists in componentMap.");
        componentMap.put(componentString, component);
    }

    /**
     * Deregister a component from the componentMap.
     * 
     * @param componentString string name of the component
     */
    public static void deregisterComponent(final String componentString) {
        if (!componentMap.containsKey(componentString))
            Logger.getGlobal().warning("Component " + componentString + " does not exist in componentMap.");
        Component comp = componentMap.get(componentString);
        if (comp.getComponentType() == CompType.ENGINE)
            ((CacheEngine) comp).cleanUpSharedVars();
        if (comp.getComponentType() == CompType.DRAM)
            ((DRAMServer) comp).cleanUpResources();
        componentMap.remove(componentString);
    }

    /**
     * Register a component to the timeline with a delay.
     * 
     * @param ts current timestamp
     * @param componentString string name of the component
     */
    public static void delayedDeregisterComponent(final long ts, final String componentString) {
        timeline.registerEvent(ts + DeregisterEvent.delay, new DeregisterEvent(componentString));
    }

    /**
     * Application reads trace file and register the next request to the timeline as a new event.
     * 
     * @param application application that reads the workload trace file
     */
    public void registerApplicationRequest(final Application application) {
        Pair<Long, Message> firstRequest = application.pollRequest();
        if (firstRequest != null)
            timeline.registerEvent(firstRequest.getKey(), firstRequest.getValue());
    }

    /**
     * Register next profile event (profile and save information every hour)
     * 
     * @param ts current timestamp (register next profile event which will be triggered after an hour)
     */
    private void addProfileEvent(final long ts) {
        if (!timeline.needToStop()) {
            final long newTs = ts + ProfileEvent.profileInterval;
            timeline.registerEvent(newTs, new ProfileEvent());
        }
    }

    /**
     * Register next reconfigure event (reconfigure every RECONFIG time)
     * 
     * @param ts current timestamp (register next RECONFIGURE event which will be triggered after RECONFIGURE time)
     */
    private void addReconfigEvent(final long ts) {
        if (!timeline.needToStop()) {
            final long newTs = ts + ReconfigEvent.oscGCInterval;
            timeline.registerEvent(newTs, new ReconfigEvent(true));
        }
    }

    /**
     * The main function of the TimelineManager:
     * 1. Reads the next event registered in the timeline.
     * 2. Handles different types of events:
     *    2.1. Message:
     *         - If the message is from the APPLICATION, it reads the trace file and registers the next request as a new event.
     *         - Calls the corresponding message handler of the component.
     *    2.2. PROFILE:
     *         - Profiles hit/miss counts, cache occupied sizes, send/receive bytes, and saves this information.
     *         - Profile events are triggered hourly (current version: manually set 1 hour as a profile interval).
     *    2.3. RECONFIGURE:
     *         - This message will be used to trigger dynamic reconfiguration events.
     * 
     * @param totalRequestCount total number of requests in the workload trace file
     */
    public void runTimeline(final long totalRequestCount) {
        // If Macaron is disabled, save profile information in local ProfileInfoStore
        boolean isMacaronEnabled = MacConf.CONTROLLER_NAME != null;
        final ProfileInfoStore profileInfoStore = !isMacaronEnabled ? new ProfileInfoStore() : null;
        final CostInfoStore costInfoStore = !isMacaronEnabled ? new CostInfoStore() : null;

        addProfileEvent(0L); // Add the first profiling event
        if (isMacaronEnabled)
            addReconfigEvent(0L); // Add the first reconfiguration event

        long requestCount = 0L, eventCount = 0L;
        Pair<Long, Event> timeEvent = null;
        Pair<Pair<Long, Event>[], Integer> newEvents = null;
        while ((timeEvent = timeline.pollEvent()) != null) {
            eventCount += 1L;
            final long ts = timeEvent.getKey();
            final Event event = timeEvent.getValue();
            newEvents = null;
            switch (event.getEventType()) {
                case MESSAGE:
                    final Message msg = (Message) event;
                    
                    // If it is a request sent from APPLICATION, register the next application request 
                    if (msg.getSrcType() == CompType.APP && msg.getFlow() == MsgFlow.SEND
                            && msg.getQType() != QType.ACK) {
                        if (msg.getMsgType() != MsgType.REQ)
                            throw new RuntimeException("Unexpected message type: " + msg.getMsgType().toString());
                        this.registerApplicationRequest((Application) componentMap.get(MacConf.APP_NAME));
                        ProgressBar.printProgressBar(totalRequestCount, requestCount++);
                    }

                    // Call the corresponding message handler of the component
                    String tgtStr = msg.getFlow() == MsgFlow.RECV ? msg.getDst() : msg.getSrc();
                    newEvents = componentMap.get(tgtStr).msgHandler(ts, msg);
                    break;

                case PROFILE:
                    if (isMacaronEnabled) {
                        // If Macaron is enabled, send the profiled information to Macaron controller
                        Controller controller = (Controller) componentMap.get(MacConf.CONTROLLER_NAME);
                        for (String compStr : componentMap.keySet()) {
                            final Component component = componentMap.get(compStr);
                            final ProfileInfo profileInfo = component.getAndResetProfileInfo();
                            controller.saveProfileInfo(ts, component.getComponentType(), compStr, profileInfo);
                            controller.calculateAddCost(ts, component.getComponentType(), compStr, profileInfo);
                        }
                        controller.saveProfileInfosToFile(ts);
                        controller.saveCostInfosToFile(ts);
                    } else {
                        // If Macaron is disabled, send profiled information to local ProfileInfoStore */
                        if (profileInfoStore == null || costInfoStore == null)
                            throw new RuntimeException("profileInfoStore and costInfoStore must not be null");
                        for (String compStr : componentMap.keySet()) {
                            final Component component = componentMap.get(compStr);
                            final ProfileInfo profileInfo = component.getAndResetProfileInfo();
                            profileInfoStore.addProfileInfo(ts, component.getComponentType(), compStr, profileInfo);
                            costInfoStore.calculateAddCost(ts, component.getComponentType(), compStr, profileInfo,
                                    null);
                        }
                        profileInfoStore.saveProfileInfosToFile(ts);
                        costInfoStore.saveCostInfosToFile(ts);
                    }
                    Application application = (Application) componentMap.get(MacConf.APP_NAME);
                    application.saveLatencyInfoToFile(ts);

                    addProfileEvent(ts);
                    break;

                case RECONFIGURE:
                    if (((ReconfigEvent) event).isInit()) {
                        newEvents = ((Controller) componentMap.get(MacConf.CONTROLLER_NAME)).initReconfig(ts);
                        addReconfigEvent(ts);
                    } else {
                        newEvents = ((Controller) componentMap.get(MacConf.CONTROLLER_NAME)).endReconfig(ts);
                    }
                    break;

                case DEREGISTER:
                    deregisterComponent(((DeregisterEvent) event).getComponentString());
                    break;

                default:
                    throw new RuntimeException("Unexpected event type: " + event.getEventType().toString());
            }
            if (newEvents != null)
                timeline.registerEvents(newEvents);
        }
        Logger.getGlobal().info("Total number of events registered to Timeline: " + eventCount);
        LatencyGenerator.waitDone();

        // For each component map, if the component is CacheEngine, run saveDebugFiles() function
        for (String compStr : componentMap.keySet()) {
            final Component component = componentMap.get(compStr);
            if (component.getComponentType() == CompType.ENGINE)
                ((CacheEngine) component).saveDebugFiles();
        }
    }
}
