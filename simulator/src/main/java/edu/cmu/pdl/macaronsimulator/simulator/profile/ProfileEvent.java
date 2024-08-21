package edu.cmu.pdl.macaronsimulator.simulator.profile;

import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.event.EventType;

public class ProfileEvent implements Event {
    public static long profileInterval = 15L * 60L * 1000L * 1000L; // XXX: profile every 15 min

    @Override
    public EventType getEventType() {
        return EventType.PROFILE;
    }
}
