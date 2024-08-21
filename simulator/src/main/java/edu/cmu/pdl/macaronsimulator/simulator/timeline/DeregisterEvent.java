package edu.cmu.pdl.macaronsimulator.simulator.timeline;

import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.event.EventType;

public class DeregisterEvent implements Event {
    public static final long delay = 60L * 1000L * 1000L; // deregister after 1 minute
    private final String componentString;

    public DeregisterEvent(String componentString) {
        this.componentString = componentString;
    }

    @Override
    public EventType getEventType() {
        return EventType.DEREGISTER;
    }

    public String getComponentString() {
        return componentString;
    }
}
