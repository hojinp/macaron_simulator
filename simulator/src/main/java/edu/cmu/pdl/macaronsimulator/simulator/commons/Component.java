package edu.cmu.pdl.macaronsimulator.simulator.commons;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.message.Message;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfo;

/**
 * Component class. Implementations of the Component are: Application, DRAMServer, CacheEngine, OSC, OSCMServer, Datalake
 */
public interface Component {
    CompType getComponentType();

    Pair<Pair<Long, Event>[], Integer> msgHandler(final long timestamp, final Message message);

    ProfileInfo getAndResetProfileInfo();

    void saveProfileInfo(final long timestamp, final CompType componentType, final String name,
            final ProfileInfo profileInfo);
}
