package edu.cmu.pdl.macaronsimulator.simulator.event;

/**
 * Three message types:
 * - MESSAGE: Event type that sends/receives message
 * - PROFILE: Event type that writes the profiled data to the log file
 * - RECONFIGURE: Not implemented. Reconfigure the MacaronCache configuration following the MacaronMaster's decision.
 */
public enum EventType {
    MESSAGE, PROFILE, RECONFIGURE, DEREGISTER
}
