package edu.cmu.pdl.macaronsimulator.simulator.timeline;

import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.event.EventType;

public class Timeline {
    long currTimestamp = -1L;
    LinkedList<Event> currEvents = null;
    TreeMap<Long, LinkedList<Event>> events = new TreeMap<>();

    public Timeline() {
    }

    /**
     * Register an event to this timeline.
     * 
     * @param timestamp timestamp in which this event should be triggered
     * @param event which event is registered to this timeline
     */
    public void registerEvent(final long timestamp, final Event event) {
        if (event.getEventType() == EventType.PROFILE) {
            events.computeIfAbsent(timestamp, k -> new LinkedList<>()).addFirst(event);
        } else {
            events.computeIfAbsent(timestamp, k -> new LinkedList<>()).add(event);
        }
    }

    /**
     * Register a list of events to this timeline.
     * 
     * @param events list of events that should be triggered later
     */
    public void registerEvents(final Pair<Pair<Long, Event>[], Integer> events) {
        Pair<Long, Event>[] eventArray = events.getKey();
        int eventCount = events.getValue();
        for (int i = 0; i < eventCount; i++)
            this.registerEvent(eventArray[i].getKey(), eventArray[i].getValue());
    }

    /**
     * Poll the event with the least timestamp.
     * 
     * @return event with the least timestamp
     */
    public Pair<Long, Event> pollEvent() {
        if (currEvents == null || currEvents.size() == 0) {
            if (currEvents != null)
                events.remove(currTimestamp);
            if (events.size() == 0)
                return null;

            Map.Entry<Long, LinkedList<Event>> firstEntry = events.firstEntry();
            currTimestamp = firstEntry.getKey();
            currEvents = firstEntry.getValue();
        }
        Event event = currEvents.pollFirst();
        return new Pair<>(currTimestamp, event);
    }

    /**
     * Check if the timeline only has events that are RECONFIGURE or PROFILE. If so, return true to stop the timeline.
     * Else, return false to keep processing other events.
     * 
     * @return whether the timeline should be stopped
     */
    public boolean needToStop() {
        int cnt = 0;
        for (LinkedList<Event> timeEvents : events.values())
            cnt += timeEvents.size();
        if (cnt == 0)
            return true;
        if (cnt == 1) {
            if (currEvents != null && currEvents.size() > 0) {
                EventType t = currEvents.get(0).getEventType();
                return t == EventType.PROFILE || t == EventType.RECONFIGURE;
            }
            for (LinkedList<Event> timeEvents : events.values()) {
                if (timeEvents.size() > 0) {
                    EventType t = timeEvents.get(0).getEventType();
                    return t == EventType.PROFILE || t == EventType.RECONFIGURE;
                }
            }
        }
        return false;
    }

    /**
     * Check if there is any event registered to this timeline.
     * 
     * @return whether there is any event registered to this timeline
     */
    public boolean isEmpty() {
        return events.size() == 0 || (events.size() == 1 && currEvents != null && currEvents.size() == 0);
    }
}
